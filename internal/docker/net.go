package docker

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// NettoolsImage is the helper image crepa builds once: alpine with iptables.
const NettoolsImage = "crepa-nettools:1"

const nettoolsDockerfile = "FROM alpine:3.20\nRUN apk add --no-cache iptables\n"

// Net cuts and restores the network between containers. For every container
// it starts a small helper that shares the container's network namespace and
// holds the iptables rules, so the containers themselves need no tools and
// no extra privileges.
type Net struct {
	Run     Runner
	Project string

	helpers map[string]string // container ID -> helper container ID
	order   []Container
}

// Attach starts one helper per container and installs an empty CREPA chain
// in front of INPUT and OUTPUT.
func (n *Net) Attach(ctx context.Context, cs []Container) error {
	if _, err := n.Run(ctx, "", "image", "inspect", NettoolsImage); err != nil {
		if _, err := n.Run(ctx, nettoolsDockerfile, "build", "-q", "-t", NettoolsImage, "-"); err != nil {
			return fmt.Errorf("building the network helper image: %w", err)
		}
	}
	if err := n.removeHelpers(ctx); err != nil {
		return err
	}
	n.helpers = map[string]string{}
	n.order = cs
	for _, c := range cs {
		out, err := n.Run(ctx, "", "run", "-d", "--rm",
			"--label", "crepa.project="+n.Project,
			"--network", "container:"+c.ID,
			"--cap-add", "NET_ADMIN",
			NettoolsImage, "sleep", "86400")
		if err != nil {
			return fmt.Errorf("starting the network helper for %s: %w", c.Name, err)
		}
		n.helpers[c.ID] = strings.TrimSpace(out)
	}
	return n.each(ctx, cs, func(Container) string {
		return "iptables -N CREPA && iptables -I INPUT -j CREPA && iptables -I OUTPUT -j CREPA"
	})
}

// Partition splits the containers into groups that cannot reach each other.
// Containers in no group keep talking to everyone. It replaces any earlier
// partition.
func (n *Net) Partition(ctx context.Context, groups [][]Container) error {
	group := map[string]int{}
	for i, g := range groups {
		for _, c := range g {
			group[c.ID] = i
		}
	}
	return n.each(ctx, n.order, func(c Container) string {
		cmds := []string{"iptables -F CREPA"}
		gi, ok := group[c.ID]
		if !ok {
			return cmds[0]
		}
		for _, peer := range n.order {
			gp, ok := group[peer.ID]
			if !ok || gp == gi {
				continue
			}
			for _, ip := range peer.IPs {
				cmds = append(cmds,
					"iptables -A CREPA -s "+ip+" -j DROP",
					"iptables -A CREPA -d "+ip+" -j DROP")
			}
		}
		return strings.Join(cmds, " && ")
	})
}

// Heal removes every rule crepa added.
func (n *Net) Heal(ctx context.Context) error {
	return n.each(ctx, n.order, func(Container) string { return "iptables -F CREPA" })
}

// Close stops the helpers.
func (n *Net) Close(ctx context.Context) error { return n.removeHelpers(ctx) }

func (n *Net) removeHelpers(ctx context.Context) error {
	out, err := n.Run(ctx, "", "ps", "-aq", "--filter", "label=crepa.project="+n.Project)
	if err != nil {
		return err
	}
	if ids := strings.Fields(out); len(ids) > 0 {
		if _, err := n.Run(ctx, "", append([]string{"rm", "-f"}, ids...)...); err != nil {
			return err
		}
	}
	return nil
}

// each runs a shell script in the helper of every container, in parallel.
func (n *Net) each(ctx context.Context, cs []Container, script func(Container) string) error {
	var wg sync.WaitGroup
	errs := make([]error, len(cs))
	for i, c := range cs {
		h, ok := n.helpers[c.ID]
		if !ok {
			return fmt.Errorf("no network helper for %s", c.Name)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := n.Run(ctx, "", "exec", h, "sh", "-c", script(c)); err != nil {
				errs[i] = fmt.Errorf("%s: %w", c.Name, err)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
