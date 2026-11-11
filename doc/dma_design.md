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
against the target schema and fail on any field that would be pruned, unless the operator accepts the
loss explicitly with `--accept-pruned-fields` (section 5.9).

### 2.3 Type changes

- New kind `ReservedIP` plus `ReservedIPList`, registered in `crd/apis/danm/v1/register.go`.
- New `DanmNet.Status` with `ReservedIPs []NetRipStatus`.
- New field `DanmNetSpec.Options.Mtu`.
- `DanmNetSpec.Options` and `DanmNetOption.Pool` lost their `omitempty` tag.
- `IpPool.Start`, `.End`, `.LastIp` had their JSON tag fixed from the misspelled `omitEmpty`
  (which never actually omitted anything) to `omitempty`.
- `DanmEpIface` gained `Mtu int`.
- `DanmEpSpec` gained `RipRefs RipReferences` (`reservedIpRefs`, with `v4` and `v6` string
  members), recording the ReservedIP objects an interface's addresses were taken from. A 4.3
  `DanmEp` has no counterpart, so a copied `DanmEp` carries it empty.
- `DanmEp` labels are no longer a plain copy of the Pod's labels. They are computed by
  `datastructs.EpLabels`: the Pod's labels, minus any `danm.io/reserved-ip-v4` and
  `danm.io/reserved-ip-v6` keys the Pod carries itself, plus those two keys set from
  `Spec.RipRefs` when non-empty. CNI ADD writes them, and `svcwatcher` re-applies them whenever the
  Pod's labels change. 4.4 lists `DanmEp`s with a label selector on these keys to decide whether a
  shared ReservedIP address is still in use when freeing it (`pkg/danmep/danmep.go`,
  `isAddressShared`). This replaced an earlier design based on CRD `selectableFields`, which would
  have raised 4.4's minimum Kubernetes version to 1.30; 4.4 has no Kubernetes version requirement
  beyond the `v1` APIs that 4.3 already needs.
- The ReservedIP webhook rejects a ReservedIP whose name, or whose `spec.network.name`, is not a
  valid label value (at most 63 characters, alphanumerics, `-`, `_` and `.`).
- The ReservedIP CRD caps `spec.network.name` at `maxLength: 63`.
- ReservedIP objects are labelled `danm.io/network-name` and `danm.io/network-type` by the webhook,
  so the objects of one network can be selected, and deleted, with a single call.

### 2.4 TenantConfig VNI bitmask

4.3 sized every `HostDevices[i].Alloc` bitmask at `MaxAllowedVni + 1` = 5001 bits regardless of
`vniType`, and its admission refused any `vniRange` above 5000 for both VLAN and VxLAN
(`pkg/admit/confadmit.go:110` and `pkg/admit/validators.go:233` at `v4.3.0`). The bitmask is stored
as `(len+7)/8` bytes, 626 bytes, and `NewBitArrayFromBase64` derives the length from the byte
count, so a 4.3 mask decodes as 5008 bits. Bit 0 is always set by `NewBitArray` as a sentinel.

4.4 sizes the bitmask per type (`getMaxAllowedVni` in `pkg/admit/utils.go`):

| `vniType` | 4.3 limit | 4.4 limit | 4.4 bitmask |
| --- | --- | --- | --- |
| `vxlan` | 5000 | 100000 (`MaxAllowedVniVxlan`) | 100001 bits |
| `vlan` | 5000 | 4094 (`MaxAllowedVniVlan`) | 4095 bits |

The TenantConfig webhook has no fast path for automated updates: every UPDATE re-validates the
`vniRange` of every `HostDevices` entry against its type's limit. That includes the updates
`confman` makes from inside the network webhook when a TenantNetwork reserves or frees a VNI
(`pkg/confman/confman.go:147`). A single VLAN entry carried over from 4.3 with a `vniRange` above
4094 therefore makes every subsequent UPDATE of the TenantConfig fail, which in turn fails every
VLAN or VxLAN TenantNetwork CREATE and DELETE that touches it.

### 2.5 Webhook changes

- `admissionregistration.k8s.io/v1beta1` becomes `v1`. `admissionReviewVersions` stays
  `["v1beta1"]`.
- The `MutatingWebhookConfiguration` carries four hooks, all scoped to `danm.io`:

  | Hook | Path | Operations | Resources | `sideEffects` |
  | --- | --- | --- | --- | --- |
  | `danm-netvalidation.danm.io` | `/netvalidation` | CREATE, UPDATE | danmnets, clusternetworks, tenantnetworks | `None` |
  | `danm-configvalidation.danm.io` | `/confvalidation` | CREATE, UPDATE | tenantconfigs | `None` |
  | `danm-netdeletion.danm.io` | `/netdeletion` | DELETE | danmnets, clusternetworks, tenantnetworks | `NoneOnDryRun` |
  | `reservedipvalidation.danm.io` | `/reservedip` | CREATE, UPDATE, DELETE | reservedips | `NoneOnDryRun` |

- `UPDATE` operations are re-enabled. 4.3 deployments usually run without the UPDATE hook.
- Network UPDATEs that only touch IPAM state are fast-tracked without validation or mutation
  (`isItAnAutomatedUpdate`, `pkg/admit/netadmit.go:246`): `Alloc` or `Alloc6` must have changed, and
  the `Spec` must be otherwise identical once `Alloc`, `Alloc6`, `Pool.LastIp`, `Pool6.LastIp`,
  `Pool6.Cidr`, `Pool6.Start` and `Pool6.End` are blanked. Only `Spec` is compared, and writes to the
  `/status` subresource never reach the webhook.
- A new validator, `validateMtuChange` (`pkg/admit/netvalidators.go:239`), refuses to lower the MTU
  of a network that has Pods connected to it.
- On DELETE of a TenantNetwork, the netdeletion hook frees its VNI in the TenantConfig, and on DELETE
  of any network it deletes the ReservedIPs labelled with that network.
- The webhook's ClusterRole gains `tenantnetworks`, `tenantnetworks/status`, `clusternetworks`,
  `clusternetworks/status` and `reservedips`.

### 2.6 Pod and Service annotation keys

This is the change most likely to break a production cluster, and it is not visible in the CRDs
at all.

The Pod annotation carrying DANM interface definitions is built from the group name:
`danmIfDefinitionSyntax = datastructs.DanmApiPath + "/interfaces"` (`pkg/metacni/metacni.go:38`). It therefore
changes from `danm.k8s.io/interfaces` to `danm.io/interfaces`. The lookup is a
`strings.Contains(key, danmIfDefinitionSyntax)` scan over the Pod's annotations, and
`"danm.k8s.io/interfaces"` does **not** contain `"danm.io/interfaces"`. After cutover, every
existing workload template silently loses its DANM networks and falls back to the default network.

Service annotations consumed by `svcwatcher` change the same way
(`pkg/svccontrol/utils.go:15-19`): `danm.k8s.io/{selector,network,tenantNetwork,clusterNetwork}`
become `danm.io/*`.

### 2.7 CNI node-local state

`integration/cni_config/00-danm.conf` is **byte-identical** between `v4.3.0` and `master`. There is
nothing to migrate in the CNI configuration directory, and `dma` must not touch it.

The only CNI-side change is `integration/cni_config/danm_rbac.yaml`: the `caas:danm` ClusterRole
moves to the new group and gains `reservedips`, `tenantnetworks/status`, `clusternetworks/status`
and `reservedips/status`. This is a cluster object, not a node file.

### 2.8 Go import path

`github.com/nokia/danm` became `github.com/danm-cni/danm`. Relevant only to how `dma` itself is
built, not to the migration.

### 2.9 Component runtime changes

- **svcwatcher** now runs leader election on a `coordination.k8s.io` Lease, and its RBAC gains
  `leases` get, create and update. Only the leader writes Endpoints, and it re-syncs all of them
  periodically (`reconcileAllEndpoints`). 4.3 has no leader election, so every replica writes. The
  4.4 binary only defines `--kubeconfig` and `--version` through the standard `flag` package, so the
  `--logtostderr` argument of the 4.3 DaemonSet makes it exit with
  `flag provided but not defined: -logtostderr`.
- **netwatcher** templates gain `priorityClassName: system-node-critical` and
  `terminationGracePeriodSeconds: 15`. Discovery now polls `ServerResourcesForGroupVersion` for up
  to one minute at start-up, and refuses to start when only one of `TenantNetwork` and
  `ClusterNetwork` is served. 4.4 netwatcher records the parent device of NADs it patched in the
  `danm.io/parent-device` and `danm.io/patched-master` annotations. NADs patched by 4.3 carry
  neither, and are resolved by the host link lookup fallback in `resolveNadParentDevice`, so there
  is nothing to migrate.
- **cleaner** from this repository uses the `danm.io` client, `danmep.DeleteDanmep` and
  `ipam.GarbageCollectIps` since commits `223d8b4` and `033a48d`, so it only works against 4.4. It
  also runs leader election on a Lease, and relies on the `DanmEp` ReservedIP labels of section 2.3
  to free shared ReservedIP addresses.

## 3. Lightweight versus production mode

DANM installs one of two mutually exclusive network management API sets:

- **lightweight**: `DanmEp`, `DanmNet`
- **production**: `DanmEp`, `TenantNetwork`, `ClusterNetwork`, `TenantConfig`, and in 4.4 also
  `ReservedIP`

`DanmEp` is present in both. `netwatcher` discovers which APIs exist only during start-up
(`pkg/netcontrol/netcontrol.go`, `discoverResources`), and never repeats discovery later in its
life cycle. DANM commit `9e059d8` ("Treat presence of production APIs as atomic") exists because a
partially discovered production API set left `netwatcher` silently serving only half the networks.
In 4.4 the same condition makes `netwatcher` refuse to start, so a partial set now crash-loops it
instead.

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

Step 6 reads the tag, validates it (integer, sane bounds, and coherent with
`VxlanOverhead` / `VxlanOverheadV6` for VxLAN networks, see `pkg/mtu/mtu.go`), and sets
`Spec.Options.Mtu` on the new-group object. An absent tag leaves `Mtu` unset, which resolves to
1500. An unparseable or out-of-range tag fails the step loudly rather than creating a quietly broken
network. The `dma.danm.io/mtu` annotation itself is stripped from the copy, since the value now
lives in the spec and a stale duplicate invites drift.

Preflight **fails** when an IPVLAN network carries no tag, overridable with `--accept-default-mtu`
to explicitly choose 1500 everywhere. A warning would be scrolled past and rediscovered later as a
fabric-wide MTU regression. The failure is cheap to clear with `dma mtu set`.

Tagging must happen before step 6 runs. 4.4's `validateMtuChange`
(`pkg/admit/netvalidators.go:239`) refuses to lower the MTU of a network that has Pods connected, so a
wrong value baked in at step 6 cannot simply be corrected downward afterwards; it needs a rollback
of step 6, a re-tag, and a re-run.

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

Two optional interfaces let a plan talk to the operator without failing anything:

```go
// Warner is implemented by a step that must warn before its confirmation.
type Warner interface {
    Warnings(context.Context, *cluster.Handle) ([]string, error)
}

// Advisor is implemented by a plan that has standing warnings for `dma plan` and `dma status`.
type Advisor interface {
    Advisories(rows []StepRow) []string
}
```

The runner prints a step's `Warnings` above its confirmation prompt, and on a dry run. A plan's
`Advisories` receive the detected and recorded state of every step and decide for themselves which
warnings currently apply; the 4.3-to-4.4 plan uses this for the warning of step 6.

Plans self-register into a registry in their `init()`. Registry validation runs at process start
and rejects a plan whose steps are not ordered by phase, whose step IDs are not contiguous from 1,
or whose final phase is not `PhaseCleanup`.

`cluster.Handle` carries the Kubernetes clientset, the apiextensions clientset, a dynamic client,
old-group and new-group DANM clients, the state store, the resolved image configuration, the
resolved binary payload, the detected mode, and the logger.

### 5.3 State

State lives in the `kube-system/dma-state` ConfigMap, one key per plan ID. The value is a JSON
document holding the detected mode, the resolved image references, the resolved workload references
of section 5.8, whether pruned fields were accepted (section 5.9), and one record per step: state,
`startedAt`, `finishedAt`, the identity that ran it, any error text, the warnings it raised, and the
workload snapshots it took. A `lastExecutedStep` pointer enforces LIFO rollback.

Steps reach their own record through the `cluster.Handle`, which the runner points at the plan and
step being run: `SaveSnapshot` and `LoadSnapshot` for the workloads a step patches in place, and
`Warnf` for findings which must reach the operator without failing the step. Warnings are printed
as they are raised and stored with the step, capped so that a step reporting a large inventory
cannot overflow the ConfigMap, and `dma status` shows them until the step runs again. Snapshots are
kept across a failed execution, so that a retry never replaces the original with a half-patched
form, and are dropped once the step is rolled back.

`Detect` exists in addition to stored state because stored state records intent while the detector
records reality. The detector wins on conflict, so a step interrupted halfway can never read as
done.

### 5.4 Backups

Per-object backups are deliberately **not** taken. The convert steps copy rather than move: the
old-group objects remain untouched and authoritative until step 14, so they are their own backup.

Workload specs that are patched in place (the `webhook`, `netwatcher`, `svcwatcher` and `cleaner`
workloads of section 5.8, the webhook configurations) **are** snapshotted into the state ConfigMap before
patching, because those are genuine in-place mutations. They are small and bounded.

Node-local CNI binaries are backed up on the node itself, next to the original, by the copier
(see step 12).

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

The components for the 4.3-to-4.4 plan are `webhook`, `netwatcher`, `svcwatcher` and `cleaner`.
`cleaner` is built from this repository; the others come from the DANM project. The
`danm-cni-plugins` image is not used; see step 12.

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

### 5.8 Workload configuration

DANM components are not deployed uniformly: their namespace, name and controller kind differ between
installations. Every workload a plan inspects or patches is therefore addressed by a reference the
operator can tailor, never by a hardcoded name.

```go
type WorkloadKind string // "DaemonSet" or "Deployment"

type WorkloadRef struct {
    Kind      WorkloadKind
    Namespace string
    Name      string
}

type WorkloadConfig struct {
    Overrides map[string]WorkloadRef // component name -> reference
}

type ComponentWorkload struct {
    Component string
    Default   WorkloadRef
    Required  bool
}
```

A plan declares its components in `PlanMeta.Workloads []ComponentWorkload`. The runner resolves them
together with the images before any step runs, so a malformed reference or an undeclared component
fails every command, including `dma plan`.

A reference is written `<Kind>/<namespace>/<name>`, for example `DaemonSet/danm/netwatcher`. Only
`DaemonSet` and `Deployment` are accepted, for every component; anything else is a configuration
error reported before any step runs.

Resolution per component: `Overrides[name]` when set, otherwise the plan's default reference for the
component. Precedence mirrors images (section 5.6): the repeatable
`--workload <component>=<Kind>/<namespace>/<name>` flag, then a `workload_<component>` key in the
`dma-config` ConfigMap, then the plan default. Naming a component the plan does not declare is an
error, so a typo cannot silently fall back to the default.

```
# dma-config
workload_netwatcher: DaemonSet/danm/netwatcher
workload_webhook: Deployment/danm/danm-webhook
```

Whether a component is required stays fixed by the plan and cannot be configured. A required
component whose resolved workload does not exist fails preflight; an optional one is reported and
skipped by every later step. There is no way to declare a required component absent.

The plan defaults for the 4.3-to-4.4 plan are the names of the DANM and danm-utils manifests:

| Component | Default reference | Required |
| --- | --- | --- |
| `webhook` | `Deployment/kube-system/danm-webhook-deployment` | yes |
| `netwatcher` | `DaemonSet/kube-system/netwatcher` | yes |
| `svcwatcher` | `DaemonSet/kube-system/svcwatcher` | no |
| `cleaner` | `DaemonSet/kube-system/danm-cleaner` | no |

Step 1 resolves every component, checks the health of each workload found, and records the resolved
references in the plan's state record together with whether each workload exists. Every later step
and every rollback reads the references from state, never from the configuration, so editing
`dma-config` or passing a different flag mid-plan cannot redirect a patch or a restore to a different
object. Changing a reference means rolling back to step 1 and running it again. `dma plan` and
`dma status` print the recorded references once step 1 is done, and the resolved ones before.

Per-kind behaviour of the steps that patch workloads is spelled out where it differs: the health
check of step 1, the all-at-once roll of `svcwatcher` in step 11.

### 5.9 Accepting pruned fields

The prune check of step 1 (section 2.2) is fatal by default: a field the 4.4 schema does not know
is silently dropped when the object is created in the new group, and `dma` cannot tell whether
some other tool relies on it. The usual remedy is to remove the field from the old-group object and
run step 1 again.

When the operator knows the fields are dead, `--accept-pruned-fields` lets the migration drop them
instead:

- Step 1 raises every finding as a warning rather than a failure. The warnings name each object and
  the paths of its pruned fields, capped like any other warning, and every one is printed.
- Step 1 records the acceptance in the plan's state record, next to the mode, together with the
  number of objects affected. Later steps read the acceptance from state, never from the flag, so it
  cannot change mid-plan. Changing the decision means rolling back to step 1 and running it again
  with or without the flag.
- The acceptance covers every kind the plan copies, `DanmEp`s included, and every object, including
  objects created or changed in the old group after step 1. It is all or nothing; there is no
  per-object or per-field acceptance.
- `--force` does not imply it, and it does not relax any other check.

Steps 6, 7 and 13 run the same prune check on every object they write, since old-group objects can
change after step 1:

- Without a recorded acceptance, an object whose copy would lose fields fails the step without its
  copy being written. The step is resumable once the field is removed from the source.
- With a recorded acceptance, the fields are pruned from the copy by `dma` itself and reported as
  warnings, so that what is written is exactly what the API server stores.

Pruning is part of the step 6 transformation, so the match check of steps 6 and 7 and the drift
detection of step 13 compare pruned forms, and a dropped field never reads as a mismatch or as
drift.

The old-group objects keep the dropped fields until step 14 deletes them, so they remain the backup
until then. While an acceptance is recorded, the confirmation prompt of step 14 repeats that the
fields of the affected objects are lost for good once it runs.

## 6. CLI

```
dma plans                                    list registered plans
dma plan --from 4.3 --to 4.4                 print the step table for a plan
dma status                                   state of the in-progress plan
dma execute  --plan 4.3-to-4.4 --step 6
dma execute  --plan 4.3-to-4.4 --step 1 --workload netwatcher=DaemonSet/danm/netwatcher
dma rollback --plan 4.3-to-4.4 --step 6
dma mtu list
dma mtu set   <kind>/<ns>/<name> <value>
dma mtu clear <kind>/<ns>/<name>
dma shim status
dma shim remove
```

Built with `spf13/cobra`. `spf13/pflag` is already an indirect dependency of the repository.

`dma plan` resolves against the live cluster, so its output shows the detected mode, which
mode-dependent steps will actually run, the resolved image URL for each image-bearing step, and the
current `Detect` state of every step. The operator reviews the list that will really execute.

While step 6 is `Done` and step 14 is not, `dma plan` and `dma status` repeat the warning of step 6
that changes to the old API group are no longer carried over.

`dma rollback` is strict LIFO: only the most recently executed step may be rolled back, and only if
it is reversible. Unwinding further means repeating the command. `--force` does not relax this.

`--accept-default-mtu` and `--accept-pruned-fields` each turn one fatal finding of step 1 into a
warning; see sections 4.4 and 5.9. Neither is relaxed by `--force`.

`--force` relaxes only the predecessor check of `dma execute`, and never for an irreversible step:
cleanup always requires every earlier step to be `Done`.

`dma shim status` and `dma shim remove` manage the annotation shim after the plan is complete; see
step 4.

## 7. Plan: 4.3 to 4.4

| # | Phase | Step | Reversible |
| --- | --- | --- | --- |
| 1 | Preflight | `verify-source-cluster` | n/a |
| 2 | Deploy | `install-new-crds` | yes |
| 3 | Deploy | `install-new-rbac` | yes |
| 4 | Deploy | `install-annotation-shim` | yes |
| 5 | Convert | `annotate-existing-pods` | yes |
| 6 | Convert | `copy-network-objects` | yes |
| 7 | Convert | `copy-danmeps` | yes |
| 8 | Convert | `dual-annotate-services` | yes |
| 9 | Convert | `replicate-reserved-ips` | yes |
| 10 | Cutover | `deploy-new-webhook` | yes |
| 11 | Cutover | `roll-control-plane` | yes |
| 12 | Cutover | `install-cni-binaries` | yes |
| 13 | Cutover | `resync-and-verify` | yes |
| 14 | Cleanup | `remove-old-api` | **no** |

### Step 1: `verify-source-cluster`

Read-only. Asserts that the old-group CRDs are present and at the 4.3 shape; that
`netwatcher`, `svcwatcher` (if deployed), `cleaner` (if deployed) and the webhook are healthy.
Detects and records the deployment mode, failing if both `DanmNet` and the production kinds exist.
Inventories and counts every object per kind. Dry-runs every source object against the target
structural schema and fails on any field that would be pruned unless `--accept-pruned-fields` is
given (section 5.9). Classifies every network for MTU
risk and fails on an untagged IPVLAN network unless `--accept-default-mtu` is given. Resolves all
component images, the pull secret, and the binary payload, and checks node architectures.

In production mode it also fails on any `TenantConfig` `HostDevices` entry of `vniType: vlan` whose
`vniRange` reaches above 4094 (section 2.4). The report names the entry and its range, every VNI
above 4094 currently set in its `alloc` bitmask, and the TenantNetworks holding those VNIs. The
operator clears it on the 4.3 side: TenantNetworks holding a VNI above 4094 are moved first, then
the `vniRange` is trimmed. `dma` only reports; it does not fix.

It validates the ReservedIP replication tags already present on `DanmEp`s with the checks of
step 9, and in lightweight mode fails on any such tag.

Implementation notes:

- Every check runs even after an earlier one failed, so the operator sees all problems at once. An
  error talking to the API server stops the step; a finding about the cluster does not.
- The pruning dry-run cannot be done by the API server, since the `danm.io` CRDs are not installed
  yet. It runs locally: each source object is pruned with the apiextensions pruning algorithm
  against the structural schema of the embedded 4.4 CRD of its kind and mode (A.3). With
  `--accept-pruned-fields` the findings are raised as warnings instead, and the acceptance is
  recorded in state (section 5.9).
- Mode detection: `DanmNet` marks lightweight, any of `TenantNetwork`, `ClusterNetwork` and
  `TenantConfig` marks production, and every CRD of the detected mode must then be present, at
  group `danm.k8s.io`, the expected kind and scope, with `v1` served and stored. When both sets are
  installed, `--mode` together with `--force` migrates the named one and leaves the other in place.
- Health: each component's workload is resolved as in section 5.8. The `webhook` and `netwatcher`
  workloads must exist; `svcwatcher` and `cleaner` are optional. A workload is healthy when its
  latest generation is observed and every desired Pod is updated and ready: for a DaemonSet,
  `updatedNumberScheduled` and `numberReady` equal `desiredNumberScheduled` with none unavailable;
  for a Deployment, `updatedReplicas` and `availableReplicas` equal `spec.replicas` with none
  unavailable. The resolved references and their presence are recorded in state with the mode.
- An MTU tag must be an integer between 68 (1280 for a network with IPv6) and 65535 minus the VxLAN
  overhead of section 4.4. An invalid tag fails preflight on any network type, not only IPVLAN.
- A ReservedIP name must be both a valid label value, as the 4.4 webhook demands, and a valid object
  name.
- The VLAN limit of 4094 is mirrored as a constant: DANM's `pkg/admit` depends on far more than `dma`
  needs.
- With `--binary-image` the payload and node architecture checks are skipped, as the binaries are not
  in the `dma` image.
- On success the step stores a report (mode, inventory counts, MTU-at-risk count, ReservedIP count,
  payload architecture) in its state record, and records the mode in the plan record. `Detect` reads
  the step as `Done` only when both are present, since the runner itself records a `--mode` value
  before a step runs. Rollback forgets the mode, so the next run detects it again.

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
`caas:danm` (including `reservedips` and the new `/status` subresources), `caas:danm-webhook`
(including `tenantnetworks`, `clusternetworks`, their `/status` subresources and `reservedips`),
`netwatcher`, `svcwatcher` (including `coordination.k8s.io` `leases` get, create and update) and
`caas:danm-cleaner`. Both groups are authorised for the duration of the migration.

This must precede step 11, whose components need these roles to start, and step 12: the new CNI
binary reads the new group, and without the role it would fail CNI ADD on an authorisation denial.

Rollback removes the added rules.

### Step 4: `install-annotation-shim`

Deploys a small mutating admission webhook, shipped in the `dma` image, that on Pod CREATE sets
`danm.io/interfaces` from `danm.k8s.io/interfaces` by the translation rule below, and adds the marker
annotation `dma.danm.io/interfaces-copied: "shim"`.

**Translation rule.** Shared by the shim, step 5, step 9 and step 13. The new value is the old value
with every entry kept as it is, except that an entry's `ip` or `ip6` is replaced by the name of a
ReservedIP that step 9 created (labelled `dma.danm.io/plan`) when that ReservedIP is in the Pod's
namespace, on the entry's network, holds an address of that family, and either:

- the entry requests that address literally, with or without a prefix length, or
- the Pod is one the address was replicated from, by namespace and name as recorded on the
  ReservedIP in step 9. This covers a dynamic request from the original Pod, and from a StatefulSet
  or bare Pod recreated under the same name.

A dynamic request from any other Pod, such as a new replica of a Deployment, is left unchanged; no
Pod identity ties it to one address. Before step 9 no such ReservedIP exists, so the rule produces a
verbatim copy. The old key is never changed, so the 4.3 CNI keeps requesting exactly what it did.

The shim reads the ReservedIPs from an informer restricted to the plan label, and its
ServiceAccount gets `get`, `list` and `watch` on `reservedips.danm.io`. When its cache is not
synced, it falls back to a verbatim copy rather than rejecting the Pod; step 13 corrects such Pods.

This is what buys zero service interruption. No workload template is edited, so no Deployment or
StatefulSet is rolled, and a Pod created at any point in the migration window carries both keys.
The 4.3 CNI matches the old key, the 4.4 CNI matches the new one, and neither key substring-matches
the other, so exactly one matches on each side.

Rewriting workload templates instead would have forced a rolling restart of every DANM-attached
workload, which is precisely the interruption this tool exists to avoid. Template cleanup becomes
the user's own chore, at their own pace, after the migration. The DANM components `dma` rolls
itself are the exception, see steps 11 and 14.

The shim only acts on Pod CREATE, so it does not cover Pods that already exist; step 5 does. It
leaves a Pod that already carries `danm.io/interfaces` untouched, so a converted template is never
overridden.

**The shim outlives the plan.** Step 14 does not remove it. Until a workload's template carries
`danm.io/interfaces`, the shim is what gives its new Pods the key the 4.4 CNI reads, and with it the
ReservedIP mapping of the translation rule. Removing it earlier would silently put every new Pod of
an unconverted workload on the default network.

- `dma shim status` lists every Pod template that still carries `danm.k8s.io/interfaces` without
  `danm.io/interfaces`: Deployments, StatefulSets, DaemonSets, CronJobs, and ReplicaSets and Jobs
  without an owner. For each, it also prints the value the translation rule would give its Pods
  today, which is what the operator should put on the template.
- `dma shim remove` deletes the shim's webhook configuration, Deployment and RBAC. It refuses to
  run before step 14 is `Done`, since rollback of step 4 is the way to remove it during the plan,
  and while `dma shim status` lists any template. It requires `remove-annotation-shim` as its typed
  token, and `--force` does not relax it. Mirror Pods are not covered by the check; their static
  manifests were already reported by step 5.

Rollback removes the webhook configuration and its Deployment.

### Step 5: `annotate-existing-pods`

Patches `danm.io/interfaces` onto every existing Pod that carries `danm.k8s.io/interfaces` but not
the new key, with the value given by the translation rule of step 4, together with the marker
annotation `dma.danm.io/interfaces-copied: "patched"`.

Without it, every Pod created before step 4 carries only the old key, which causes two problems:

- After step 12, kubelet re-runs CNI ADD against the same Pod object whenever it recreates the Pod's
  sandbox, for example after a node reboot. The 4.4 binary matches only the new key, so the Pod
  silently falls back to the default network. CNI DEL is unaffected, since it finds its `DanmEp`s
  by container ID and never reads the annotation.
- Step 14 refuses to run while any Pod carries only the old key. Without this step, the only way to
  meet that gate would be to restart every DANM-attached workload.

Annotating a running Pod does not restart it. Its UID and containers are unchanged: controllers
own Pods by label and template-hash label and never compare a live Pod's annotations against its
template, and kubelet restarts a container only when its spec hash changes, which annotations are
not part of. The step therefore writes a merge patch touching only `metadata.annotations`, never
labels or spec.

The step must run after step 4 is `Done`, so that once the list is taken any Pod created later
gets the new key from the shim; nothing slips through between the two. Pods with a
`deletionTimestamp` are skipped. Mirror Pods of static Pods are not patched, since the API server
copy is not their source of truth; they are reported, and the operator adds the key to the static
manifest on the node.

Idempotent and resumable: a Pod already carrying the new key is skipped, whether the shim, this
step, or the user put it there. The list of patched Pods is not recorded in state, for the same
1 MiB reason as step 6; the marker annotation is the record.

`dma` only ever rewrites a `danm.io/interfaces` value on a Pod carrying the marker, with either
value. A new key the user set by hand is never touched.

Rollback removes `danm.io/interfaces` and the marker from every Pod whose marker is `patched`.
Pods marked `shim` keep theirs, as do Pods with a hand-set new key.

### Step 6: `copy-network-objects`

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
- drop every field the 4.4 schema prunes, only when step 1 recorded an acceptance of pruned fields;
  without one, such an object fails the step instead (section 5.9)
- for every `TenantConfig` `HostDevices` entry of `vniType: vxlan`, resize the `alloc` bitmask to
  `MaxAllowedVniVxlan + 1` = 100001 bits, keeping every set bit (section 2.4): decode with
  `bitarray.NewBitArrayFromBase64`, create the target with `bitarray.NewBitArray`, set each bit that
  is set in the source, and `Encode()`. Bit 0, the sentinel, is set in both. Bits 5001 to 5007 of
  the source are padding and are never set.
- copy the `alloc` bitmask of `vniType: vlan` entries verbatim at its 4.3 size of 5001 bits, and
  entries without a `vniType` or with an empty `alloc` unchanged

Idempotent and resumable: an existing, matching target is skipped. Matching compares bitmasks by
their set bits, not by their base64 encoding, since a resized VxLAN bitmask never matches its source
byte for byte. Source `resourceVersion`s are deliberately not recorded: step 13 detects drift by
content, and one record per `DanmEp` would not fit the 1 MiB limit of the state ConfigMap on a large
cluster.

Before asking for confirmation, the step prints a warning that from this point on, changes to
`danm.k8s.io` network objects and `TenantConfig` (CREATE, UPDATE and DELETE) are not carried over to
`danm.io`: only IP and VNI allocation state is synchronised, by step 13, and every other change made
after step 6 is executed is lost at cutover. `dma status` and `dma plan` repeat the warning for as
long as step 6 is `Done` and step 14 is not. The old group is not locked: 4.3 deployments usually run
without the UPDATE hook, so there is no admission path to enforce a freeze through, and the new group
is not locked either.

Rollback deletes the new-group copies. The old group is still live and authoritative, so the risk is
nil.

### Step 7: `copy-danmeps`

The same transformation for `DanmEp`, kept as a separate step because `DanmEp` is the
high-cardinality kind, one per Pod interface, and the one carrying Pod `ownerReferences` that must
be preserved. Separating it lets a large cluster retry just this part. `DanmEpIface.Mtu` is left
unset; it is populated by 4.4 when an interface is next created. `DanmEpSpec.RipRefs` is left empty.

The copy's labels are computed with `datastructs.EpLabels` from the source labels and the empty
`RipRefs`, exactly as 4.4 would compute them (section 2.3). This only differs from a verbatim copy
when a Pod carries a `danm.io/reserved-ip-v4` or `danm.io/reserved-ip-v6` label itself; such a
label is dropped, so that it cannot make a copied `DanmEp` count as a user of a ReservedIP.

The four ReservedIP replication annotations of step 9 are stripped from the copy; step 9 reads them
from the old-group `DanmEp`.

`DanmEp`s are not covered by the warning of step 6: CNI keeps creating and deleting them during the
window, and step 13 synchronises them in full.

### Step 8: `dual-annotate-services`

Patches `danm.io/{selector,network,tenantNetwork,clusterNetwork}` onto Services alongside the
existing `danm.k8s.io/*` annotations. An in-place patch with no restart; `svcwatcher` of either
version finds its own keys.

Rollback removes the added annotations.

### Step 9: `replicate-reserved-ips`

Turns addresses that Pods hold today into 4.4 ReservedIP objects, so that a workload can keep its
address across restarts after the migration. Production mode only: 4.4 ignores ReservedIPs on
`DanmNet`s.

**Tagging.** The operator selects addresses by annotating **old-group** `DanmEp`s, one annotation
per address family and sharing mode. The value is the name of the ReservedIP to create:

```
dma.danm.io/reserved-ip-v4: <name>
dma.danm.io/reserved-ip-v6: <name>
dma.danm.io/shared-reserved-ip-v4: <name>
dma.danm.io/shared-reserved-ip-v6: <name>
```

- `reserved-ip-v4` and `reserved-ip-v6` replicate `Spec.Iface.Address` and
  `Spec.Iface.AddressIPv6` respectively into a non-shareable entry.
- The `shared-` variants replicate the same addresses into a `shareable: true` entry. All `DanmEp`s
  holding the same address on the same network name the same ReservedIP.
- A `DanmEp` carries at most one annotation per address family.
- The v4 and v6 annotations of one `DanmEp` may name the same ReservedIP, which then holds both
  addresses; 4.4 allows at most one address per family in a ReservedIP. Sharing is per address, so
  a ReservedIP may hold a shareable v4 and a non-shareable v6 address.

The ReservedIP is created in the `DanmEp`'s namespace. 4.4 only lets a Pod reference a ReservedIP of
its own namespace.

**Why `dma` creates the objects itself.** The 4.4 webhook refuses to create a ReservedIP for an
address whose bit is already set in the network's `alloc` (`checkReservationConflicts`). That is
true of every address in use by a Pod, so once step 10 is live these objects can no longer be
created. Like the copies of steps 6 and 7, they are written while no webhook covers `danm.io`, and
`dma` performs what the webhook would otherwise do:

- labels each ReservedIP with `danm.io/network-name` and `danm.io/network-type`
- adds it to `Status.ReservedIPs` of its new-group network, through the `/status` subresource
- sets `Spec.Description` to name the source `DanmEp`s, and adds the label `dma.danm.io/plan` with the
  plan ID, which is how rollback and the shim find the objects
- records the namespace and name of every Pod whose `DanmEp` was tagged for it in the
  `dma.danm.io/source-pods` annotation, a JSON list, used by the translation rule of step 4

**Validation.** Every tag is validated before anything is written; any error fails the step with no
change made. Step 1 runs the same checks on the tags present at that time, as early feedback.

- The name is a valid label value, and not `dynamic`.
- The `DanmEp`'s network is a `TenantNetwork` or `ClusterNetwork` whose name is a valid label value.
  A `ClusterNetwork` also needs the `reservedIp` privilege in the `TenantConfig`.
- The tagged family has an address on the `DanmEp`, it is not `none`, it lies inside the
  network's CIDR for that family, and the network is L3 for that family (non-empty `alloc` or
  `alloc6`).
- All `DanmEp`s naming one ReservedIP are on the same network. They agree on the address of each
  family, and on whether it is shared.
- A non-shared name is used by exactly one `DanmEp` for its family.
- No two ReservedIPs on one network claim the same address.
- No ReservedIP of that name already exists, unless it is an identical one from an earlier run of
  this step.

The `alloc` bitmask is not checked or changed. The address is already allocated by the `DanmEp`
holding it, and stays so until that `DanmEp` is deleted.

**Shared addresses.** Every new-group `DanmEp` copy under a `shared-` tag gets the ReservedIP's name
in `Spec.RipRefs.V4` or `.V6`, and its labels are recomputed with `datastructs.EpLabels`. 4.4 then
counts these `DanmEp`s as users of the address (`isAddressShared`) and frees the address only when
the last one is deleted. Copies under a non-shared tag keep empty `RipRefs`, exactly as 4.4 records
a non-shareable ReservedIP allocation.

**Pod annotations.** 4.4 skips a reserved address in dynamic allocation, and refuses a literal
static request for it, so a Pod gets the address only when its `danm.io/interfaces` entry references
the ReservedIP by name. Once the ReservedIPs exist, the step re-applies the translation rule of
step 4 to every Pod carrying the marker and patches those whose value changes. That covers the
Pods patched by step 5 and the Pods the shim admitted since step 4, neither of which could see a
ReservedIP yet. From then on the shim applies the rule to every new Pod.

The step reports, without failing, every Pod controller whose template requests a replicated
address dynamically and whose Pods do not keep their names, such as a Deployment: its new Pods are
not mapped by the rule and come back with a different address. The operator changes such templates
to reference the ReservedIP by name. Templates referencing it, or requesting it literally, also
need to be on the new key before `dma shim remove`, like the template cleanup of step 4.

Idempotent and resumable: existing identical ReservedIPs and already-set `RipRefs` are skipped.
Tags added or changed after this step are not replicated, see step 13.

Rollback clears the `RipRefs` set by this step and recomputes the labels of those `DanmEp` copies.
It removes the ReservedIPs from their networks' `Status.ReservedIPs`, then deletes every
ReservedIP labelled with the plan ID. It then re-applies the translation rule to every marked Pod,
which with the ReservedIPs gone restores a verbatim copy of the old value. LIFO rollback guarantees that the 4.4 webhook of step 10 is
not live at that point, so nothing tries to validate the deletions.

### Step 10: `deploy-new-webhook`

Patches the `webhook` workload recorded by step 1 to the resolved 4.4 image, and replaces the
`MutatingWebhookConfiguration` with the `admissionregistration.k8s.io/v1` form carrying the four
hooks of section 2.5: `danm-netvalidation.danm.io`, `danm-configvalidation.danm.io`,
`danm-netdeletion.danm.io` and `reservedipvalidation.danm.io`, with `sideEffects` and
`admissionReviewVersions` set, `UPDATE` enabled, rules scoped to `danm.io`, and the CA bundle
regenerated.

The webhook goes first so the new API is validated before anything begins writing to it in anger.
The copies of steps 6 and 7 and the ReservedIPs of step 9 were written before it existed, so they
were neither validated nor mutated; in particular no VNI was allocated twice.

The full prior specs are snapshotted into state. The step waits for Ready with a timeout and
auto-reverts to the snapshot on timeout, which is the real defence against a mistyped image URL.

### Step 11: `roll-control-plane`

Patches the `netwatcher` workload recorded by step 1, and the `svcwatcher` and `cleaner` workloads
if step 1 found them, to their resolved 4.4 images. Components that are not present are skipped and
reported, not created:
`svcwatcher` and `cleaner` are optional in both modes.

Changing the image alone is not enough; each template is brought to its 4.4 form:

- `netwatcher`: `priorityClassName: system-node-critical` and `terminationGracePeriodSeconds: 15`.
- `svcwatcher`: the `--logtostderr` argument is removed, since the 4.4 binary exits on it
  (section 2.9).
- every rolled component whose template carries `danm.k8s.io/interfaces` gets `danm.io/interfaces`
  added next to it, with the same value. Nothing else about the annotation changes. Both keys are
  needed because step 11 runs before step 12: a rolled Pod lands on a node still running the 4.3
  binary, which only matches the old key, and later on the 4.4 binary, which only matches the new
  one. With only the new key the Pod would silently fall back to the default network until step 12.
  The old key is removed in step 14.

`svcwatcher` is rolled all at once, and its rollout strategy is restored from the snapshot
afterwards. A DaemonSet gets its `updateStrategy` temporarily set to `RollingUpdate` with
`maxUnavailable: 100%` and `maxSurge: 0`; a Deployment gets its `strategy` temporarily set to
`Recreate`, which removes every old Pod before creating a new one. A rolling update would leave 4.3
replicas, which all write Endpoints, running next to the 4.4 leader, which re-syncs all of them,
with the two versions resolving the dual-annotated Services of step 8 against different `DanmEp`
groups. Rolling all at once leaves a few seconds with no `svcwatcher` at all; Endpoints keep their
last state meanwhile, so traffic is not affected, only updates in that window are delayed. Rollback
restores the snapshot the same way.

Same snapshot, readiness-wait and auto-revert behaviour as step 10.

### Step 12: `install-cni-binaries`

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

### Step 13: `resync-and-verify`

Reconciles every object whose old-group content has drifted from its new-group copy since steps 6
and 7, since 4.3 was still allocating IPs during the window. Then asserts cluster-wide health: all new-group components
Ready, `netwatcher` serving the expected API set, no node left on an old binary.

The 4.4 webhook is live by now, so the shape of every write matters (section 2.5):

- **Networks**: only the IPAM fields are written: `alloc`, `alloc6`, `Pool.LastIp` and
  `Pool6.LastIp`. Such an UPDATE always takes the webhook's fast path, even when the old object's
  spec drifted in other fields as well, so allocation state can never be blocked by validation.
- **TenantConfig**: only the `alloc` bitmask of each `HostDevices` entry is written, resized for
  `vxlan` entries exactly as in step 6. This UPDATE is fully validated, which passes once step 1's
  VLAN range check has passed.
- **DanmEps**: synchronised in full, creates and deletes included, with labels computed as in
  step 7. The webhook does not cover them. A `DanmEp` created in the old group after step 9 gets
  `RipRefs` only when it carries a `shared-` tag naming a ReservedIP created by step 9 with the same
  network and address. Any other tag added after step 9 is reported, not replicated: the 4.4
  webhook now refuses to create a ReservedIP for an address in use.
- **Pods**: any Pod still carrying only `danm.k8s.io/interfaces` is patched exactly as in step 5,
  and the translation rule of step 4 is re-applied to every marked Pod. Neither is expected to
  change anything, but this covers a Pod admitted while the shim was briefly unavailable or its
  ReservedIP cache was not synced.

Drift is detected by applying the full step 6 transformation to the current old-group object and
comparing the result to the new-group copy, ignoring IPAM fields. Re-applying the transformation
means a tagged MTU never reads as drift, so the resync can never trip `validateMtuChange` by
appearing to lower an MTU to 0.

Non-IPAM changes are not applied. The step reports them, without failing, as the changes lost
according to the warning of step 6: network and `TenantConfig` objects whose spec drifted outside
IPAM fields, and objects created in or deleted from the old group after step 6. The operator applies
whatever is still wanted to the new group by hand.

### Step 14: `remove-old-api`

Runs in this order:

1. removes the old webhook configuration, so no 4.3 admission logic, such as VNI freeing on network
   DELETE, can fire on the objects about to disappear. The annotation shim stays; see step 4
2. deletes the old-group CRDs belonging to the detected mode by exact name, which cascades to all
   old-group objects, along with the old RBAC
3. removes `danm.k8s.io/interfaces` from the templates of the components rolled in step 11, leaving
   `danm.io/interfaces` in place; this restarts them, which is safe now that every node runs the 4.4
   binary
4. removes the node-local `.dma-bak-<planID>` files via one final copier run

When step 1 recorded an acceptance of pruned fields, the confirmation prompt states how many objects
lose fields for good, since the old-group objects deleted here are their last copy (section 5.9).

**Irreversible.** Requires the plan ID as its typed confirmation token, and refuses to run unless
steps 1 to 13 are all `Done`, every node reports the new binary installed, no Pod carries only the
old annotation, and no template of a component rolled in step 11 carries only the old key. A node
still running a 4.3 `danm` binary reads `danm.k8s.io`; deleting that group would take its Pod
networking down completely, and that is exactly the failure cleanup must not be able to cause.

CRDs are deleted only by exact name, from the detected mode's list, never by group. A stray CRD from
the other mode is left in place and reported rather than silently dropped, and any other CRD in the
`danm.k8s.io` group, such as `danmnetworkpolicies.danm.k8s.io`, is never touched.

## 8. Deployment

`integration/manifests/dma/dma.yaml` provides the Pod, ServiceAccount, ClusterRole and
ClusterRoleBinding. The ClusterRole needs:

- `apiextensions.k8s.io` CustomResourceDefinitions: full access
- `danm.k8s.io` and `danm.io` all resources: full access
- `apps` DaemonSets and Deployments: get, list, patch, create, delete
- `apps` StatefulSets and ReplicaSets, `batch` CronJobs: get, list, for `dma shim status`
- `admissionregistration.k8s.io` Mutating and Validating webhook configurations: full access
- `rbac.authorization.k8s.io` ClusterRoles and ClusterRoleBindings: full access
- core ConfigMaps in `kube-system`: full access, for state and config
- core Pods, Nodes, Services, Secrets: get, list, patch, and create and delete for Pods, for the
  canary and the copier
- `batch` Jobs: create, get, list, delete, for the canary copier and `dma shim status`

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
- 4.4 is the `master` branch HEAD, at the time of writing commit `2ea72cb` ("Replace DanmEp
  selectableFields with label based filtering").

Every delta documented in section 2 was derived by diffing those two revisions. The Go module cache
contains only the single pinned commit, so the clone is required to inspect 4.3 at all.

### A.2 The `go.mod` pin follows the 4.4 baseline

`danm-utils/go.mod` pins the 4.4 baseline of A.1:

```
github.com/danm-cni/danm v0.0.0-20261111105820-2ea72cb3eb13
```

It contains the ReservedIP API, the `GarbageCollectIps` based freeing `cleaner` depends on, the
netwatcher and svcwatcher refactors, the October 2026 security updates, the `danm.io` ReservedIP
label fix, and the label based `isAddressShared`. Neither the embedded CRDs nor the payload binaries
need CRD field selectors any more.

The `/payload/4.4` binaries are built from the pinned module (section 5.7), and the embedded CRDs are
copied from the same commit (A.3), so a stale pin ships a stale CNI binary and stale schemas. **Bump
`go.mod` and refresh the embedded CRDs together whenever the 4.4 baseline in A.1 moves.** A unit test
of the plan fails if an embedded CRD declares `selectableFields` again.

### A.3 Where the embedded artifacts come from

`dma` embeds manifests taken from the DANM clone at 4.4, and from this repository for `cleaner`:

| Artifact | Source path |
| --- | --- |
| New CRDs, lightweight | DANM `integration/crds/lightweight/{DanmEp,DanmNet}.yaml` |
| New CRDs, production | DANM `integration/crds/production/{DanmEp,TenantNetwork,ClusterNetwork,TenantConfig,ReservedIP}.yaml` |
| CNI ClusterRole | DANM `integration/cni_config/danm_rbac.yaml` |
| Webhook Deployment and configuration | DANM `integration/manifests/webhook/webhook.yaml` |
| netwatcher RBAC and template | DANM `integration/manifests/netwatcher/{0netwatcher_rbac,netwatcher_ds}.yaml` |
| svcwatcher RBAC and template | DANM `integration/manifests/svcwatcher/{0svcwatcher_rbac,svcwatcher_ds}.yaml` |
| cleaner RBAC | danm-utils `integration/manifests/cleaner/cleaner.yaml` |

The CRDs are embedded verbatim under `pkg/dma/plans/v43to44/manifests/crds/<mode>/`. Step 1 reads
their schemas for its pruning check, step 2 applies them.

The `.yaml.tmpl` variants use `confd`-style `getenv` interpolation for the image reference. `dma`
resolves images itself (section 5.6) and so embeds the plain `.yaml` forms, patching the image
field programmatically rather than templating. The DaemonSet templates are used only as the
reference for the fields step 11 changes; step 11 patches the live DaemonSets and never replaces them.

The 4.3 equivalents of the same files, from the `v4.3.0` tag, are what step 14 must delete and what
rollback of steps 3, 10 and 11 must restore.

### A.4 Other `danm.k8s.io` CRDs are out of scope

A cluster may hold CRDs in the `danm.k8s.io` group that are not part of DANM core, such as
`danmnetworkpolicies.danm.k8s.io`. `dma` must not touch them, which is why step 14 deletes CRDs only
by exact name from the detected mode's list, never by group.

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
- Existing Pods are annotated in place (step 5), not restarted; annotating a live Pod does not
  restart it.
- Addresses become ReservedIPs only when the operator tags their old-group `DanmEp`, and the tag's
  value names the ReservedIP (step 9). `dma` creates them before the 4.4 webhook goes live, since
  the webhook refuses an address already in use. Workload templates are not changed to reference
  them; Pod annotations are, through the translation rule shared by the shim and the Pod patching
  (step 4).
- No per-object backups; convert steps copy rather than move, so old objects are their own backup.
  Workload specs patched in place are still snapshotted.
- `spf13/cobra` for subcommands.
- State in a ConfigMap, with `Detect` authoritative over stored state on conflict.
- Rollback is strict LIFO, most-recently-executed step only.
- Confirmation is a typed token: step name normally, plan ID for irreversible cleanup.
- Preflight hard-fails on an untagged IPVLAN network, overridable with `--accept-default-mtu`.
- `cleaner` is part of the migration and is rolled in step 11. `policer` is not part of DANM 4.3 and is
  out of scope.
- Components rolled in step 11 keep their interfaces annotation unchanged apart from the key: step 11
  adds `danm.io/interfaces` next to `danm.k8s.io/interfaces`, step 14 removes the old key.
- `svcwatcher` is rolled all at once, never mixing 4.3 and 4.4 replicas.
- TenantConfig `alloc` bitmasks are resized to 100001 bits for `vxlan` entries only; `vlan` entries
  keep their 4.3 size. Preflight hard-fails on a `vlan` `vniRange` above 4094.
- No Kubernetes version or API availability check at all: 4.4 finds the users of a ReservedIP
  through `DanmEp` labels, not CRD field selectors, and the `apiextensions.k8s.io/v1` and
  `admissionregistration.k8s.io/v1` APIs it needs have been GA since Kubernetes 1.16.
- Neither API group is locked during the migration. Non-IPAM changes made to the old group after
  step 6 are lost; step 6 warns about this, and step 13 reports the changes it dropped.
- Step 14 removes the old webhook configuration before deleting the old CRDs. The annotation shim
  stays after the plan completes, and is removed with `dma shim remove` once no workload template
  carries only the old key.
- Component workloads are tailorable by `<Kind>/<namespace>/<name>` reference, through a
  `workload_<component>` key in `dma-config` or the `--workload` flag, with the plan's manifest names
  as defaults (section 5.8). Every component may be a DaemonSet or a Deployment. Which components are
  required is fixed by the plan. References are resolved once by step 1 and read from state afterwards.
- Preflight hard-fails on any field the 4.4 schema would prune, overridable with
  `--accept-pruned-fields`. The acceptance is all or nothing, recorded in state by step 1, applies to
  every later copy and resync, and is not implied by `--force` (section 5.9).
