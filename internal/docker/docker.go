// Package docker drives a docker compose project and cuts the network between
// its containers. It shells out to the docker CLI.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// Runner runs a docker command and returns its stdout. Tests replace it.
type Runner func(ctx context.Context, stdin string, args ...string) (string, error)

// CLI runs the real docker binary.
func CLI(ctx context.Context, stdin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if msg == "" {
			msg = strings.TrimSpace(out.String())
		}
		return out.String(), fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, msg)
	}
	return out.String(), nil
}

// Container is one running container of the project.
type Container struct {
	ID      string
	Name    string // compose service name, plus "-N" when the service has replicas
	Service string
	Command string
	Image   string
	IPs     []string
}

// IP returns the container's first address.
func (c Container) IP() string {
	if len(c.IPs) == 0 {
		return ""
	}
	return c.IPs[0]
}

// Project is a compose project crepa owns for the length of a run.
type Project struct {
	Files []string
	Name  string
	Run   Runner
}

func (p *Project) compose(args ...string) []string {
	out := []string{"compose"}
	for _, f := range p.Files {
		out = append(out, "-f", f)
	}
	return append(append(out, "-p", p.Name), args...)
}

// Up starts the project from scratch: anything left from an earlier run is
// removed first, volumes included.
func (p *Project) Up(ctx context.Context) error {
	if err := p.Down(ctx); err != nil {
		return err
	}
	_, err := p.Run(ctx, "", p.compose("up", "-d", "--quiet-pull")...)
	return err
}

// Down removes the project's containers, networks and volumes.
func (p *Project) Down(ctx context.Context) error {
	_, err := p.Run(ctx, "", p.compose("down", "-v", "--remove-orphans", "-t", "1")...)
	return err
}

// Containers lists the project's running containers.
func (p *Project) Containers(ctx context.Context) ([]Container, error) {
	out, err := p.Run(ctx, "", p.compose("ps", "-q")...)
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(out)
	if len(ids) == 0 {
		return nil, fmt.Errorf("compose project %s has no running containers", p.Name)
	}
	out, err = p.Run(ctx, "", append([]string{"inspect"}, ids...)...)
	if err != nil {
		return nil, err
	}
	return parseInspect(out)
}

type inspect struct {
	ID     string `json:"Id"`
	Name   string
	Config struct {
		Image  string
		Cmd    []string
		Labels map[string]string
	}
	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress string
		}
	}
}

func parseInspect(out string) ([]Container, error) {
	var raw []inspect
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		return nil, fmt.Errorf("docker inspect: %v", err)
	}
	perService := map[string]int{}
	for _, r := range raw {
		perService[r.Config.Labels["com.docker.compose.service"]]++
	}
	var cs []Container
	for _, r := range raw {
		svc := r.Config.Labels["com.docker.compose.service"]
		name := svc
		if perService[svc] > 1 {
			name = svc + "-" + r.Config.Labels["com.docker.compose.container-number"]
		}
		c := Container{
			ID:      r.ID,
			Name:    name,
			Service: svc,
			Image:   r.Config.Image,
			Command: strings.Join(r.Config.Cmd, " "),
		}
		nets := make([]string, 0, len(r.NetworkSettings.Networks))
		for n := range r.NetworkSettings.Networks {
			nets = append(nets, n)
		}
		sort.Strings(nets)
		for _, n := range nets {
			if ip := r.NetworkSettings.Networks[n].IPAddress; ip != "" {
				c.IPs = append(c.IPs, ip)
			}
		}
		cs = append(cs, c)
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].Name < cs[j].Name })
	return cs, nil
}
