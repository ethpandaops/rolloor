# rolloor

Staged, health-gated rollouts for a fleet of containers. One instance per environment.

rolloor watches the image tags a set of containers run, and when a tag moves it converges the containers toward the new digest a batch at a time, under a weight budget, comparing each batch against the containers it hasn't reached yet. When a batch does worse, it stops and leaves that batch running for debugging. It knows nothing about what the containers are: what a target is, how to see what it runs, how to update it, whether it is ready, and how to compare it with the rest all arrive as a file it reads and five programs it runs.

- `tasks/prd.md` is the spec this is built from: inputs, the reconcile loop, hooks, API, storage.
- `examples/generic/` runs the whole loop against six nginx containers on a local registry. CI runs it on every change.
- `contrib/ethpandaops/` holds the first real workload's scripts and targets template.

## Run the example

```sh
./examples/generic/run.sh
```

Needs docker, curl and jq. It builds four variants of an image, watches rolloor move six containers between them in waves and batches, halts on the broken one, and converges past it.

## Develop

```sh
make test     # go test -race
make cover    # statement coverage floors per package
make lint     # golangci-lint, plus the workload-word lint on the Go tree
make words    # the word lint alone
```

The Go tree must never mention the workload. `scripts/lint-words.sh` fails the build if it does.

## Configure

`config.yaml` names the environment, the targets directory, the hooks directory, the budget, the presets and the registry. Each target in `targets.d/*.yaml` declares its node, weight, image, labels and, optionally, which probe programs to run for it. See the spec for every field.

```sh
rolloor validate --config config.yaml
rolloor hook inspect --target app1/web --config config.yaml
rolloor serve --config config.yaml
```
