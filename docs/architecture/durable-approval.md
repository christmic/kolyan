# Durable approval architecture

```text
model step
   |
   v
policy plan ---- allow ------------------> execute grant
   |
   +---- require approval --> checkpoint --> store --> process may exit
                                                   |
user decision ------------------------------------+
                                                   v
                         load -> validate -> fresh grant -> tool once
                                                   |
                                                   v
                                           next model step
```

`TurnControl` is not part of this durable flow. It may cancel or approve an
in-process compatibility execution, but it is never serialized and cannot be
used to represent a long-lived approval.

The checkpoint is deliberately a continuation, not a serialized executor. It
contains data needed to reconstruct the next operation while provider clients,
tasks, sockets, locks, and memory state are discarded. This keeps the recovery
boundary explicit and prevents duplicate model requests or tool execution.
