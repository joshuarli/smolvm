#ifndef SMOLVM_SWIFT_FFI_H
#define SMOLVM_SWIFT_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque, process-local owner of smolvm's embedded runtime. */
typedef struct smolvm_swift_runtime smolvm_swift_runtime;
typedef struct smolvm_swift_stream smolvm_swift_stream;

/**
 * Creates an embedded smolvm runtime.
 *
 * `options_json` currently accepts an optional object with `stateDirectory`,
 * `libDirectory`, `agentRootfs`, and `bootBinary` absolute paths. The returned runtime never invokes the smolvm
 * CLI or daemon. All error/result strings are UTF-8 JSON and must be freed
 * with `smolvm_swift_string_free`.
 */
smolvm_swift_runtime *smolvm_swift_runtime_create(
    const char *options_json,
    char **error_json
);

void smolvm_swift_runtime_free(smolvm_swift_runtime *runtime);

/** All machine request/result values use the documented JSON ABI. */
int32_t smolvm_swift_machine_create(
    smolvm_swift_runtime *runtime,
    const char *request_json,
    char **error_json
);
int32_t smolvm_swift_machine_start(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);
/** Starts the image-defined ENTRYPOINT/CMD as the machine's durable workload. */
int32_t smolvm_swift_machine_start_image_workload(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);
int32_t smolvm_swift_machine_stop(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);
int32_t smolvm_swift_machine_delete(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);

/** Returns `{ "state": String, "running": Bool, "pid": Int32? }`. */
char *smolvm_swift_machine_status(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);

/**
 * Runs a non-interactive agent command. The result is
 * `{ "exitCode": Int32, "stdoutBase64": String, "stderrBase64": String }`.
 */
char *smolvm_swift_machine_exec(
    smolvm_swift_runtime *runtime,
    const char *name,
    const char *request_json,
    char **error_json
);

/** Starts a live, non-interactive exec stream. */
smolvm_swift_stream *smolvm_swift_machine_exec_stream_start(
    smolvm_swift_runtime *runtime,
    const char *name,
    const char *request_json,
    char **error_json
);

/**
 * Gets one event, `{ kind: stdout|stderr|exit|error|pending|finished, ... }`.
 * A `pending` event means no output arrived during `timeout_millis`.
 */
char *smolvm_swift_stream_next(
    smolvm_swift_stream *stream,
    uint64_t timeout_millis,
    char **error_json
);

void smolvm_swift_stream_free(smolvm_swift_stream *stream);

/** Pulls an image and lists images cached by a running machine. */
char *smolvm_swift_image_pull(
    smolvm_swift_runtime *runtime,
    const char *name,
    const char *reference,
    char **error_json
);
char *smolvm_swift_image_list(
    smolvm_swift_runtime *runtime,
    const char *name,
    char **error_json
);

void smolvm_swift_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
