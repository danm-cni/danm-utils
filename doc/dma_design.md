# DANM Migration Assistant (`dma`) - Design

## 1. Purpose

`dma` is a command line utility, deployed as a Kubernetes Pod with privileged access to its local
cluster's API server, which orchestrates the in-place migration of a DANM installation from one
major release to another, without interrupting the network service of already running Pods.

A migration is expressed as a **plan**: an ordered, pre-baked list of **steps** between two specific
DANM CNI project release versions. The operator lists the steps of a plan, then executes or rolls
back individual steps one at a time, each behind an explicit typed confirmation. The last phase of
every plan is always cleanup.

The first implemented plan migrates DANM 4.3 to 4.4. At the time of writing 4.4 is unreleased, so
the HEAD of the DANM project's `master` branch is treated as 4.4.

The framework is deliberately generic: adding a future plan means adding one package, with no
changes to the engine.

## 2. Why the tool is needed: the 4.3 to 4.4 delta

Verified by diffing `v4.3.0` against `master` in the DANM repository.

### 2.1 API group rename

The API group was renamed from `danm.k8s.io` to `danm.io` (`crd/apis/danm/register.go`). The
version stays `v1`. Because the group is part of a CRD's resource name, every CRD is a different
object after the rename:

| 4.3 | 4.4 |
| --- | --- |
| `danmnets.danm.k8s.io` | `danmnets.danm.io` |
| `danmeps.danm.k8s.io` | `danmeps.danm.io` |
| `tenantnetworks.danm.k8s.io` | `tenantnetworks.danm.io` |
| `clusternetworks.danm.k8s.io` | `clusternetworks.danm.io` |
| `tenantconfigs.danm.k8s.io` | `tenantconfigs.danm.io` |
| - | `reservedips.danm.io` (new) |

There is no Kubernetes-native conversion path between two different API groups. Objects must be
read from the old group and written to the new one.

### 2.2 CRD manifest modernisation

4.3 CRDs are `apiextensions.k8s.io/v1beta1` with a single `version:` field and a non-structural
`validation:` schema. 4.4 CRDs are `apiextensions.k8s.io/v1` with a `versions:` list and
structural schemas.

**This is a data-loss trap.** `v1beta1` CRDs without a structural schema do not prune unknown
fields, so a 4.3 cluster may be storing fields that the 4.4 schema does not know about. On create
against the new CRD those fields are silently dropped. Preflight must dry-run every source object
against the target schema and fail on any field that would be pruned.

### 2.3 Type changes

- New kind `ReservedIP` plus `ReservedIPList`, registered in `crd/apis/danm/v1/register.go`.
- New `DanmNet.Status` with `ReservedIPs []NetRipStatus`.
- New field `DanmNetSpec.Options.Mtu`.
- `DanmNetSpec.Options` and `DanmNetOption.Pool` lost their `omitempty` tag.
- `IpPool.Start`, `.End`, `.LastIp` had their JSON tag fixed from the misspelled `omitEmpty`
  (which never actually omitted anything) to `omitempty`.
- `DanmEpIface` gained `Mtu int`.

### 2.4 Webhook changes

- `admissionregistration.k8s.io/v1beta1` becomes `v1`.
- Webhook names `danm-netvalidation.nokia.k8s.io` / `danm-configvalidation.nokia.k8s.io` become
  `*.danm.io`.
- `sideEffects` and `admissionReviewVersions` are now mandatory and present.
- `UPDATE` operations are re-enabled (they were temporarily removed in 4.3).
- A new validator, `validateMtuChange`, refuses to lower the MTU of a network that has Pods
  connected to it.

### 2.5 Pod and Service annotation keys

This is the change most likely to break a production cluster, and it is not visible in the CRDs
at all.

The Pod annotation carrying DANM interface definitions is built from the group name:
`danmIfDefinitionSyntax = danmApiPath + "/interfaces"` (`pkg/metacni/metacni.go:39`). It therefore
changes from `danm.k8s.io/interfaces` to `danm.io/interfaces`. The lookup is a
`strings.Contains(key, danmIfDefinitionSyntax)` scan over the Pod's annotations, and
`"danm.k8s.io/interfaces"` does **not** contain `"danm.io/interfaces"`. After cutover, every
existing workload template silently loses its DANM networks and falls back to the default network.

Service annotations consumed by `svcwatcher` change the same way
(`pkg/svccontrol/utils.go:14-17`): `danm.k8s.io/{selector,network,tenantNetwork,clusterNetwork}`
become `danm.io/*`.

### 2.6 CNI node-local state

`integration/cni_config/00-danm.conf` is **byte-identical** between `v4.3.0` and `master`. There is
nothing to migrate in the CNI configuration directory, and `dma` must not touch it.

The only CNI-side change is `integration/cni_config/danm_rbac.yaml`: the `caas:danm` ClusterRole
moves to the new group and gains `reservedips`, `tenantnetworks/status`, `clusternetworks/status`
and `reservedips/status`. This is a cluster object, not a node file.

### 2.7 Go import path

`github.com/nokia/danm` became `github.com/danm-cni/danm`. Relevant only to how `dma` itself is
built, not to the migration.

## 3. Lightweight versus production mode

DANM installs one of two mutually exclusive network management API sets:

- **lightweight**: `DanmEp`, `DanmNet`
- **production**: `DanmEp`, `TenantNetwork`, `ClusterNetwork`, `TenantConfig`, and in 4.4 also
  `ReservedIP`

`DanmEp` is present in both. `netwatcher` discovers which APIs exist by probing a `List` call per
kind during start-up (`pkg/netcontrol/netcontrol.go`), and never retries discovery later in its
life cycle. DANM commit `9e059d8` ("Treat presence of production APIs as atomic") exists precisely
because a partially discovered production API set leaves `netwatcher` silently serving only half
the networks.

`dma` honours this invariant:

- Preflight detects the mode from the CRDs actually present in the source cluster, and treats the
  simultaneous presence of `DanmNet` and `TenantNetwork`/`ClusterNetwork` as a fatal, non-migratable
  condition.
- The detected mode is recorded in state and every later step reads it from there, so the mode
  cannot drift mid-plan.
- Production CRD installation is atomic: all five are applied and every one must reach
  `Established` before the step reports success; any failure rolls the whole set back rather than
  leaving a partial API set discoverable.
- A `--mode` flag exists only to override detection. Overriding to a mode that contradicts the
  detected source objects additionally requires `--force`.

## 4. MTU migration

### 4.1 The problem

4.3 resolved interface MTU differently per network type:

- **IPVLAN (DANM native)** took the MTU directly from the parent device:
  `MTU: iface.Attrs().MTU` (`pkg/danmep/ep.go:66` at `v4.3.0`).
- **MACVLAN (delegated via cnidel)** hardcoded `macvlanConfig.MTU = 1500`
  (`pkg/cnidel/cniconfs.go:64` at `v4.3.0`).
- **Other delegated CNIs** were never given an MTU at all.

4.4 resolves MTU centrally through `pkg/mtu`: `GetMtuForNet` returns `Spec.Options.Mtu`, or
`DefaultMtu` (1500) when it is unset. The delegate's raw config is only patched with `mtu` when
`Options.Mtu > 0` (`pkg/cnidel/cniconfs.go:37`).

Consequences for a migrated network with `Mtu` left unset:

| Network type | 4.3 effective MTU | 4.4 unset | Risk |
| --- | --- | --- | --- |
| IPVLAN | parent device MTU | 1500 | **Silent drop on any jumbo-frame fabric** |
| MACVLAN | 1500 | 1500 | none |
| Other delegates | delegate default | delegate default | none |

### 4.2 Why `dma` cannot auto-detect the value

`DanmEpIface.Mtu` is new in 4.4, so 4.3 `DanmEp` objects carry no record of the MTU actually in
use. A Pod-resident tool also cannot read per-node device MTUs. The operator, who knows the fabric,
is the only sound source of truth. `dma` identifies the at-risk set; it does not guess.

### 4.3 Tagging mechanism

The operator tags the **old-group** network object with the annotation:

```
dma.danm.io/mtu: "9000"
```

The annotation lives on the object being migrated because it survives Pod restarts, is reviewable
with plain `kubectl get danmnet -o yaml`, is RBAC-scoped exactly like the network it describes, and
is read by the copy step with no second lookup. The key cannot collide with DANM's own parsing:
`metacni`'s `strings.Contains(key, "danm.io/interfaces")` does not match it, and `svccontrol`
compares annotation keys exactly.

Subcommands:

```
dma mtu list                             inventory and risk table
dma mtu set   <kind>/<ns>/<name> 9000    write the annotation
dma mtu clear <kind>/<ns>/<name>         remove it
```

`dma mtu list` prints kind, namespace, name, `NetworkType`, `host_device`/`device_pool`, `vxlan`,
the 4.3 effective-MTU class, the current tag, the resulting 4.4 `Mtu`, and a risk verdict. These
are ordinary annotation edits, so they are reversible and need no typed token, but each prints a
before and after.

### 4.4 Where it takes effect

Step 5 reads the tag, validates it (integer, sane bounds, and coherent with
`VxlanOverhead` / `VxlanOverheadV6` for VxLAN networks, see `pkg/mtu/mtu.go`), and sets
`Spec.Options.Mtu` on the new-group object. An absent tag leaves `Mtu` unset, which resolves to
1500. An unparseable or out-of-range tag fails the step loudly rather than creating a quietly broken
network. The `dma.danm.io/mtu` annotation itself is stripped from the copy, since the value now
lives in the spec and a stale duplicate invites drift.

Preflight **fails** when an IPVLAN network carries no tag, overridable with `--accept-default-mtu`
to explicitly choose 1500 everywhere. A warning would be scrolled past and rediscovered later as a
fabric-wide MTU regression. The failure is cheap to clear with `dma mtu set`.

Tagging must happen before step 5 runs. 4.4's `validateMtuChange`
(`pkg/admit/validators.go:287`) refuses to lower the MTU of a network that has Pods connected, so a
wrong value baked in at step 5 cannot simply be corrected downward afterwards; it needs a rollback
of step 5, a re-tag, and a re-run.

## 5. Architecture

### 5.1 Repository layout

```
cmd/dma/dma.go                thin main, cobra root command
pkg/dma/engine/               Plan and Step interfaces, registry, runner, confirmation
pkg/dma/state/                ConfigMap-backed state store
pkg/dma/cluster/              shared clients and resolvers
pkg/dma/plans/v43to44/        the first plan, one file per step
integration/manifests/dma/    Pod, ServiceAccount, ClusterRole, ClusterRoleBinding
scm/build/Dockerfile          new `dma` target, carries /payload/4.4/{danm,fakeipam}
build_dma.sh
```

This mirrors the existing `cmd/<name>` plus `pkg/<name>` convention used by `cleaner` and
`policer`.

### 5.2 Core interfaces

```go
type Phase int

const (
    PhasePreflight Phase = iota
    PhaseDeploy
    PhaseConvert
    PhaseCutover
    PhaseCleanup
)

type StepState int // StateNotStarted, StatePartial, StateDone

type StepMeta struct {
    ID          int
    Name        string
    Description string
    Phase       Phase
    Reversible  bool
}

type Step interface {
    Meta() StepMeta
    Detect(context.Context, *cluster.Handle) (StepState, error)
    Execute(context.Context, *cluster.Handle) error
    Rollback(context.Context, *cluster.Handle) error
}

type PlanMeta struct {
    ID            string   // "4.3-to-4.4"
    From, To      string
    Description   string
    Components    []string // image-bearing workloads this plan rolls
    BinaryPayload string   // "4.4" -> /payload/4.4/{danm,fakeipam}
}

type Plan interface {
    Meta() PlanMeta
    Steps() []Step
}
```

Plans self-register into a registry in their `init()`. Registry validation runs at process start
and rejects a plan whose steps are not ordered by phase, whose step IDs are not contiguous from 1,
or whose final phase is not `PhaseCleanup`.

`cluster.Handle` carries the Kubernetes clientset, the apiextensions clientset, a dynamic client,
old-group and new-group DANM clients, the state store, the resolved image configuration, the
resolved binary payload, the detected mode, and the logger.

### 5.3 State

State lives in the `kube-system/dma-state` ConfigMap, one key per plan ID. The value is a JSON
document holding the detected mode, the resolved image references, and one record per step: state,
`startedAt`, `finishedAt`, the identity that ran it, any error text, and references to workload
snapshots. A `lastExecutedStep` pointer enforces LIFO rollback.

`Detect` exists in addition to stored state because stored state records intent while the detector
records reality. The detector wins on conflict, so a step interrupted halfway can never read as
done.

### 5.4 Backups

Per-object backups are deliberately **not** taken. The convert steps copy rather than move: the
old-group objects remain untouched and authoritative until step 12, so they are their own backup.

Workload specs that are patched in place (the webhook Deployment, the `netwatcher` and `svcwatcher`
DaemonSets, the webhook configurations) **are** snapshotted into the state ConfigMap before
patching, because those are genuine in-place mutations. They are small and bounded.

Node-local CNI binaries are backed up on the node itself, next to the original, by the copier
(see step 10).

### 5.5 Confirmation

Every `execute` and `rollback` requires the operator to type an exact token:

- normal steps: the step name
- irreversible steps (cleanup): the plan ID

A typed token rather than `y/N` because muscle memory defeats `y/N` precisely where it matters most.
Input that is not a TTY and does not supply the token is a hard error, so a scripted run can never
half-execute.

### 5.6 Image configuration

```go
type ComponentImage struct {
    Image      string            // full URL, including digest pins
    PullPolicy corev1.PullPolicy
}

type ImageConfig struct {
    RegistryPrefix string                    // "my-reg.example.com/ns/", trailing slash
    Tag            string                    // default "latest"
    PullSecret     string                    // name of an existing Secret
    PullPolicy     corev1.PullPolicy         // default IfNotPresent
    Overrides      map[string]ComponentImage // component name -> full URL
}
```

Resolution per component: `Overrides[name].Image` when set, otherwise
`RegistryPrefix + defaultName + ":" + Tag`. A full URL override always wins, which allows
digest-pinned references.

Precedence: CLI flag, then the `kube-system/dma-config` ConfigMap, then plan defaults. The
ConfigMap carries the bulk because that is what a Pod-deployed tool can realistically be fed, and
it mirrors the existing `danm-installer-config` pattern. Flags exist for one-off overrides without
editing cluster state.

The components for the 4.3-to-4.4 plan are `webhook`, `netwatcher` and `svcwatcher`. The
`danm-cni-plugins` image is not used; see step 10.

The engine resolves every declared component during preflight, before anything is modified, and
fails there on an unresolvable reference or a named pull secret that does not exist. Resolved
references are written into the state record at execute time, so `dma status` and `dma rollback`
report exactly what was applied rather than what the config says now.

### 5.7 Binary payload

The `dma` image builds `danm` and `fakeipam` from the `github.com/danm-cni/danm` version pinned in
`danm-utils`' `go.mod`, placing them under `/payload/<version>/`. A plan names the payload it needs
via `PlanMeta.BinaryPayload`. Future plans add their own payload directory to the image.

Preflight verifies that the declared payload directory exists and its contents are executable, and
compares each node's `status.nodeInfo.architecture` against the payload architecture, failing on
mismatch rather than letting it surface as a crash-looping copier. Mixed-architecture clusters need
a multi-architecture `dma` image or a per-architecture `--binary-image`.

`--binary-image` overrides the copier to use an externally built image instead of the `dma` image.

## 6. CLI

```
dma plans                                    list registered plans
dma plan --from 4.3 --to 4.4                 print the step table for a plan
dma status                                   state of the in-progress plan
dma execute  --plan 4.3-to-4.4 --step 5
dma rollback --plan 4.3-to-4.4 --step 5
dma mtu list
dma mtu set   <kind>/<ns>/<name> <value>
dma mtu clear <kind>/<ns>/<name>
```

Built with `spf13/cobra`. `spf13/pflag` is already an indirect dependency of the repository.

`dma plan` resolves against the live cluster, so its output shows the detected mode, which
mode-dependent steps will actually run, the resolved image URL for each image-bearing step, and the
current `Detect` state of every step. The operator reviews the list that will really execute.

`dma rollback` is strict LIFO: only the most recently executed step may be rolled back, and only if
it is reversible. Unwinding further means repeating the command.

## 7. Plan: 4.3 to 4.4

| # | Phase | Step | Reversible |
| --- | --- | --- | --- |
| 1 | Preflight | `verify-source-cluster` | n/a |
| 2 | Deploy | `install-new-crds` | yes |
| 3 | Deploy | `install-new-rbac` | yes |
| 4 | Deploy | `install-annotation-shim` | yes |
| 5 | Convert | `copy-network-objects` | yes |
| 6 | Convert | `copy-danmeps` | yes |
| 7 | Convert | `dual-annotate-services` | yes |
| 8 | Cutover | `deploy-new-webhook` | yes |
| 9 | Cutover | `roll-control-plane` | yes |
| 10 | Cutover | `install-cni-binaries` | yes |
| 11 | Cutover | `resync-and-verify` | yes |
| 12 | Cleanup | `remove-old-api` | **no** |

### Step 1: `verify-source-cluster`

Read-only. Asserts that the API server serves `apiextensions.k8s.io/v1` and
`admissionregistration.k8s.io/v1`; that the old-group CRDs are present and at the 4.3 shape; that
`netwatcher`, `svcwatcher` (if deployed) and the webhook are healthy. Detects and records the
deployment mode, failing if both `DanmNet` and the production kinds exist. Inventories and counts
every object per kind. Dry-runs every source object against the target structural schema and fails
on any field that would be pruned. Classifies every network for MTU risk and fails on an untagged
IPVLAN network unless `--accept-default-mtu` is given. Resolves all component images, the pull
secret, and the binary payload, and checks node architectures.

### Step 2: `install-new-crds`

Applies the `danm.io` CRDs for the detected mode:

- lightweight: `danmeps`, `danmnets`
- production: `danmeps`, `tenantnetworks`, `clusternetworks`, `tenantconfigs`, `reservedips`

Purely additive; the `danm.k8s.io` group is untouched and remains authoritative. The production set
is applied atomically and gated on every CRD reaching `Established`; any failure rolls the whole set
back rather than exposing a partial API set to `netwatcher` discovery. The step also asserts it is
not about to create the opposite mode's set in the new group.

Rollback deletes the new CRDs, which is safe while they hold no objects.

### Step 3: `install-new-rbac`

Adds ClusterRoles and ClusterRoleBindings for the new group alongside the existing ones, covering
`caas:danm` (including `reservedips` and the new `/status` subresources), `caas:danm-webhook`,
`netwatcher` and `svcwatcher`. Both groups are authorised for the duration of the migration.

This must precede step 10: the new CNI binary reads the new group, and without the role it would
fail CNI ADD on an authorisation denial.

Rollback removes the added rules.

### Step 4: `install-annotation-shim`

Deploys a small mutating admission webhook, shipped in the `dma` image, that on Pod CREATE copies
`danm.k8s.io/interfaces` to `danm.io/interfaces`.

This is what buys zero service interruption. No workload template is edited, so no Deployment or
StatefulSet is rolled, and a Pod created at any point in the migration window carries both keys.
The 4.3 CNI matches the old key, the 4.4 CNI matches the new one, and neither key substring-matches
the other, so exactly one matches on each side.

Rewriting workload templates instead would have forced a rolling restart of every DANM-attached
workload, which is precisely the interruption this tool exists to avoid. Template cleanup becomes
the user's own chore, at their own pace, after the migration.

Rollback removes the webhook configuration and its Deployment.

### Step 5: `copy-network-objects`

Reads every old-group network object for the detected mode (`DanmNet`, or `TenantNetwork` plus
`ClusterNetwork` plus `TenantConfig`) and creates the new-group equivalent.

Transformation rules:

- rewrite `apiVersion` to `danm.io/v1`
- preserve `metadata.name`, namespace, labels and annotations
- drop `resourceVersion`, `uid`, `creationTimestamp`, `generation`, and `ownerReferences` that
  point at old-group objects
- emit `Options` and `allocation_pool` explicitly, since they lost `omitempty`
- copy the `alloc` bitmask and `lastIp` verbatim; this is the live IPAM state and the single thing
  that must not drift
- apply the `dma.danm.io/mtu` tag to `Spec.Options.Mtu`, then strip the annotation from the copy
- leave `Status.ReservedIPs` empty, as 4.3 has no counterpart

Idempotent and resumable: an existing, matching target is skipped. Each source object's
`resourceVersion` is recorded in state for step 11.

Rollback deletes the new-group copies. The old group is still live and authoritative, so the risk is
nil.

### Step 6: `copy-danmeps`

The same transformation for `DanmEp`, kept as a separate step because `DanmEp` is the
high-cardinality kind, one per Pod interface, and the one carrying Pod `ownerReferences` that must
be preserved. Separating it lets a large cluster retry just this part. `DanmEpIface.Mtu` is left
unset; it is populated by 4.4 when an interface is next created.

### Step 7: `dual-annotate-services`

Patches `danm.io/{selector,network,tenantNetwork,clusterNetwork}` onto Services alongside the
existing `danm.k8s.io/*` annotations. An in-place patch with no restart; `svcwatcher` of either
version finds its own keys.

Rollback removes the added annotations.

### Step 8: `deploy-new-webhook`

Patches the webhook Deployment to the resolved 4.4 image and replaces the
`MutatingWebhookConfiguration` with the `admissionregistration.k8s.io/v1` form: renamed hooks
`danm-netvalidation.danm.io` and `danm-configvalidation.danm.io`, `sideEffects` and
`admissionReviewVersions` set, `UPDATE` re-enabled, rules scoped to `danm.io`, CA bundle
regenerated.

The webhook goes first so the new API is validated before anything begins writing to it in anger.

The full prior specs are snapshotted into state. The step waits for Ready with a timeout and
auto-reverts to the snapshot on timeout, which is the real defence against a mistyped image URL.

### Step 9: `roll-control-plane`

Patches the `netwatcher` DaemonSet, and `svcwatcher` if it is deployed, to their resolved 4.4
images. Components that are not present are skipped and reported, not created: `svcwatcher` is
optional in both modes.

Same snapshot, readiness-wait and auto-revert behaviour as step 8.

### Step 10: `install-cni-binaries`

The upstream `danm-cni` DaemonSet is not used. It is outdated, it is not what real deployments run,
and its entrypoint also writes CNI configuration files, which this migration must not do: the
4.3 and 4.4 `00-danm.conf` are byte-identical.

Instead `dma` creates a short-lived **copier DaemonSet** running the `dma` image itself, mounting
exactly one hostPath, `/opt/cni/bin`. There is no `/etc/cni/net.d` mount.

Per node the copier:

1. backs up the existing `danm` and `fakeipam` to `danm.dma-bak-<planID>` and
   `fakeipam.dma-bak-<planID>`
2. writes the new binaries to `_danm` and `_fakeipam`, then atomically renames them into place, so
   a half-written binary is never executable
3. writes a marker file that its readiness probe checks
4. sleeps

`dma` waits for `numberReady == desiredNumberScheduled`, then deletes the DaemonSet.

Pod spec: no `hostNetwork`, no `privileged`, `readOnlyRootFilesystem`, one hostPath mount.

**Canary.** The copy itself does not disturb running Pods, since the binary is only invoked on CNI
ADD and DEL, but a bad binary would break new Pod creation fleet-wide. So the step first copies to a
single node using a Job pinned by `nodeName`, creates a canary Pod on a migrated network on that
node, asserts it receives an IP from the new-group object's pool, and deletes it. Only then does the
copier DaemonSet roll across the remaining nodes. `--no-canary` skips this.

Rollback runs the same copier in restore mode, renaming each `.dma-bak-<planID>` back over the live
name. Keeping the backup node-local avoids ever having to store binaries in cluster state.

### Step 11: `resync-and-verify`

Reconciles any object whose old-group `resourceVersion` has moved since steps 5 and 6, since 4.3 was
still allocating IPs during the window. Then asserts cluster-wide health: all new-group components
Ready, `netwatcher` serving the expected API set, no node left on an old binary.

### Step 12: `remove-old-api`

Deletes the old-group CRDs, which cascades to all old-group objects, along with the old RBAC, the
old webhook configuration, the annotation shim, and the node-local `.dma-bak-<planID>` files via one
final copier run.

**Irreversible.** Requires the plan ID as its typed confirmation token, and refuses to run unless
steps 1 to 11 are all `Done`, every node reports the new binary installed, and no Pod carries only
the old annotation. A node still running a 4.3 `danm` binary reads `danm.k8s.io`; deleting that
group would take its Pod networking down completely, and that is exactly the failure cleanup must
not be able to cause.

Only CRDs belonging to the detected mode are deleted. A stray CRD from the other mode is left in
place and reported rather than silently dropped.

## 8. Deployment

`integration/manifests/dma/dma.yaml` provides the Pod, ServiceAccount, ClusterRole and
ClusterRoleBinding. The ClusterRole needs:

- `apiextensions.k8s.io` CustomResourceDefinitions: full access
- `danm.k8s.io` and `danm.io` all resources: full access
- `apps` DaemonSets and Deployments: get, list, patch, create, delete
- `admissionregistration.k8s.io` Mutating and Validating webhook configurations: full access
- `rbac.authorization.k8s.io` ClusterRoles and ClusterRoleBindings: full access
- core ConfigMaps in `kube-system`: full access, for state and config
- core Pods, Nodes, Services, Secrets: get, list, patch, and create and delete for Pods, for the
  canary and the copier
- `batch` Jobs: create, get, delete, for the canary copier

The Pod itself needs no host mounts; the copier DaemonSet it creates carries the `/opt/cni/bin`
mount.

The operator runs subcommands with `kubectl exec`.

## 9. Build

A `dma` target is added to `scm/build/Dockerfile`, building `cmd/dma` from this repository and
`cmd/danm` plus `cmd/fakeipam` from the pinned `github.com/danm-cni/danm` module into
`/payload/4.4/`. `build_dma.sh` at the repository root follows the pattern of `build_policer.sh`.

## 10. Open items for future plans

- Adding a plan means adding a `pkg/dma/plans/<id>/` package and, if it rolls different binaries, a
  payload directory in the image. No engine change.
- A future plan that migrates between two versions of the same API group could use a conversion
  webhook rather than the copy approach; the `Step` interface does not constrain this.
- Multi-architecture `dma` images are not addressed by the first plan beyond the preflight check.

## Appendix A: Implementation context

Facts established while this design was written that are not derivable from the `danm-utils`
repository alone. An implementer starting from a clean context needs these.

### A.1 Source of truth for DANM versions

A local clone of the DANM project exists at `/home/levo/work/danm`.

- 4.3 is the `v4.3.0` tag.
- 4.4 is the `master` branch HEAD, at the time of writing commit `d2699a3` ("Adding ReservedIPs
  API").

Every delta documented in section 2 was derived by diffing those two revisions. The Go module cache
contains only the single pinned commit, so the clone is required to inspect 4.3 at all.

### A.2 The `go.mod` pin is behind master and must be bumped

`danm-utils/go.mod` currently pins:

```
github.com/danm-cni/danm v0.0.0-20260923131811-3c5259632778
```

That is commit `3c52596` ("Fixing DANM IPAM relevance checks and last IP handling"), which is
`master~1`. It **predates** `d2699a3`, the commit that added the ReservedIP API.

What the pinned module **does** contain: `pkg/mtu`, `DanmNetSpec.Options.Mtu`, `DanmEpIface.Mtu`.

What it **does not** contain: `ReservedIP`, `ReservedIPList`, `DanmNetStatus`, `NetRipStatus`, and
`integration/crds/production/ReservedIP.yaml`.

Consequence: the production CRD set described in step 2, and the type references in section 2.3,
do not compile against the current dependency. **Bump `go.mod` to a pseudo-version including
`d2699a3` before implementing step 2 or anything touching the new types.**

### A.3 Where the embedded artifacts come from

`dma` embeds manifests taken from the DANM clone at 4.4:

| Artifact | Source path in the DANM repository |
| --- | --- |
| New CRDs, lightweight | `integration/crds/lightweight/{DanmEp,DanmNet}.yaml` |
| New CRDs, production | `integration/crds/production/{DanmEp,TenantNetwork,ClusterNetwork,TenantConfig,ReservedIP}.yaml` |
| CNI ClusterRole | `integration/cni_config/danm_rbac.yaml` |
| Webhook Deployment and configuration | `integration/manifests/webhook/webhook.yaml` |
| netwatcher RBAC | `integration/manifests/netwatcher/0netwatcher_rbac.yaml` |
| svcwatcher RBAC | `integration/manifests/svcwatcher/0svcwatcher_rbac.yaml` |

The `.yaml.tmpl` variants use `confd`-style `getenv` interpolation for the image reference. `dma`
resolves images itself (section 5.6) and so embeds the plain `.yaml` forms, patching the image
field programmatically rather than templating.

The 4.3 equivalents of the same files, from the `v4.3.0` tag, are what step 12 must delete and what
rollback of steps 3, 8 and 9 must restore.

### A.4 `DanmNetworkPolicy` is out of scope

`danm-utils` owns its own CRD, `DanmNetworkPolicy`, in `crd/api/netpol/`. Its group constant in
`crd/api/netpol/register.go` is still `danm.k8s.io`:

```go
const (
  GroupName = "danm.k8s.io"
)
```

This is the policer's own API, not part of DANM core, and it was **not** renamed by the 4.4 change.
`dma` must not touch it, and step 12 must not delete it. Do not "fix" this constant as part of
implementing `dma`.

### A.5 Dependencies to add

- `github.com/spf13/cobra` as a direct dependency. `github.com/spf13/pflag` is already present as
  an indirect one.
- Building `danm` and `fakeipam` into the image requires those `main` packages from the pinned
  `github.com/danm-cni/danm` module. The repository already uses the `hack/tools.go` pattern for
  build-only dependencies.

### A.6 Repository conventions to follow

- Binaries live in `cmd/<name>/<name>.go` as thin mains; logic lives in `pkg/<name>/`.
- Each binary has a root-level `build_<name>.sh`; `build_policer.sh` is the better template, as it
  handles `LATEST_TAG` and `COMMIT_HASH` ldflags injection.
- Images are built as named targets in `scm/build/Dockerfile`.
- Deployment manifests live in `integration/manifests/<name>/`.
- Existing mains use the standard library `flag` package and inject `version` and `commitHash` via
  ldflags. `dma` uses cobra instead, but should keep the same version-injection convention.

### A.7 Decisions already settled

Recorded here so they are not relitigated:

- Annotation shim, not workload template rewriting (step 4, with the reasoning in that section).
- No per-object backups; convert steps copy rather than move, so old objects are their own backup.
  Workload specs patched in place are still snapshotted.
- `spf13/cobra` for subcommands.
- State in a ConfigMap, with `Detect` authoritative over stored state on conflict.
- Rollback is strict LIFO, most-recently-executed step only.
- Confirmation is a typed token: step name normally, plan ID for irreversible cleanup.
- Preflight hard-fails on an untagged IPVLAN network, overridable with `--accept-default-mtu`.
