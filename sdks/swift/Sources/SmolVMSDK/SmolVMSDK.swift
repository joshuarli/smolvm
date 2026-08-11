import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

/// Failures at the versioned native smolvm boundary.
public enum SmolVMError: Error, Equatable, Sendable, LocalizedError {
    case nativeLibraryUnavailable(URL, String)
    case nativeSymbolMissing(String)
    case nativeFailure(String)
    case invalidResponse(String)
    case executionNotStarted
    case executionFinishedWithoutExit

    public var errorDescription: String? {
        switch self {
        case .nativeLibraryUnavailable(let url, let reason):
            "could not load smolvm native library at \(url.path): \(reason)"
        case .nativeSymbolMissing(let symbol):
            "smolvm native library is missing required symbol \(symbol)"
        case .nativeFailure(let message):
            message
        case .invalidResponse(let message):
            "invalid response from smolvm native library: \(message)"
        case .executionNotStarted:
            "smolvm execution has not been started"
        case .executionFinishedWithoutExit:
            "smolvm execution ended without an exit status"
        }
    }
}

/// File locations for one embedded smolvm host.
///
/// The native library is the small `smolvm-swift-ffi` dynamic library, not the
/// `smolvm` command-line executable. `libDirectory` must contain the matching
/// libkrun and libkrunfw artifacts; `agentRootfs` and `stateDirectory` are
/// explicit so the SDK never depends on `HOME` or a global application-support
/// directory. These paths are process-wide in smolvm and must be identical for
/// every runtime created by a host process.
public struct SmolVMRuntimeConfiguration: Sendable, Equatable {
    public let nativeLibrary: URL
    public let stateDirectory: URL
    public let libDirectory: URL
    public let agentRootfs: URL
    public let bootHelper: URL

    public init(
        nativeLibrary: URL,
        stateDirectory: URL,
        libDirectory: URL,
        agentRootfs: URL,
        bootHelper: URL
    ) {
        self.nativeLibrary = nativeLibrary.standardizedFileURL
        self.stateDirectory = stateDirectory.standardizedFileURL
        self.libDirectory = libDirectory.standardizedFileURL
        self.agentRootfs = agentRootfs.standardizedFileURL
        self.bootHelper = bootHelper.standardizedFileURL
    }
}

public struct SmolVMHostMount: Codable, Sendable, Equatable {
    public let source: String
    public let target: String
    public let readOnly: Bool

    public init(source: URL, target: String, readOnly: Bool) {
        self.source = source.standardizedFileURL.path
        self.target = target
        self.readOnly = readOnly
    }
}

public struct SmolVMPortMapping: Codable, Sendable, Equatable {
    public let host: UInt16
    public let guest: UInt16

    public init(host: UInt16, guest: UInt16) {
        self.host = host
        self.guest = guest
    }
}

/// A deliberate host↔guest Unix-domain socket bridge. Unlike a TCP port
/// forward, the bridge is carried by libkrun/vsock and has no IP listener.
/// `expose` publishes a guest listener to the specified host path; `mount`
/// presents a host listener inside the guest.
public struct SmolVMPublishedSocket: Codable, Sendable, Equatable {
    public enum Direction: String, Codable, Sendable {
        case expose
        case mount
    }

    public let direction: Direction
    public let guestPath: String
    public let hostPath: String

    public init(direction: Direction, guestPath: String, hostPath: String) {
        self.direction = direction
        self.guestPath = guestPath
        self.hostPath = hostPath
    }
}

public struct SmolVMResources: Codable, Sendable, Equatable {
    public let cpus: UInt8?
    public let memoryMiB: UInt32?
    public let network: Bool?
    public let storageGiB: UInt64?
    public let overlayGiB: UInt64?

    public init(
        cpus: UInt8? = nil,
        memoryMiB: UInt32? = nil,
        network: Bool? = nil,
        storageGiB: UInt64? = nil,
        overlayGiB: UInt64? = nil
    ) {
        self.cpus = cpus
        self.memoryMiB = memoryMiB
        self.network = network
        self.storageGiB = storageGiB
        self.overlayGiB = overlayGiB
    }
}

/// Persistent virtual-machine configuration understood by smolvm's embedded
/// runtime. An image is executed by a separate `SmolVMExecution`; creating the
/// machine itself only creates its durable VM record.
public struct SmolVMMachineSpecification: Codable, Sendable, Equatable {
    public let name: String
    public let image: String?
    public let mounts: [SmolVMHostMount]
    public let ports: [SmolVMPortMapping]
    public let publishedSockets: [SmolVMPublishedSocket]
    /// Host-side path for smolvm's dedicated Docker socket bridge. When set,
    /// the guest agent proxies host connections to `/var/run/docker.sock`.
    /// This is separate from `publishedSockets`: the dedicated bridge is the
    /// stable Docker service contract used by a bare VM running dockerd.
    public let dockerSocket: URL?
    /// Shell commands run once after the first successful VM boot. For a bare
    /// machine these run directly in the guest namespace, which is useful for
    /// preparing a persistent service such as dockerd before the socket bridge
    /// is consumed by the host.
    public let initCommands: [String]
    public let resources: SmolVMResources?
    public let persistent: Bool

    public init(
        name: String,
        image: String?,
        mounts: [SmolVMHostMount] = [],
        ports: [SmolVMPortMapping] = [],
        publishedSockets: [SmolVMPublishedSocket] = [],
        dockerSocket: URL? = nil,
        initCommands: [String] = [],
        resources: SmolVMResources? = nil,
        persistent: Bool = true
    ) {
        self.name = name
        self.image = image
        self.mounts = mounts
        self.ports = ports
        self.publishedSockets = publishedSockets
        self.dockerSocket = dockerSocket?.standardizedFileURL
        self.initCommands = initCommands
        self.resources = resources
        self.persistent = persistent
    }

    // Foundation's synthesized URL Codable representation is a URL string
    // (`file:///…`), while the native ABI deliberately accepts filesystem
    // paths. Keep the public API typed as URL but send the path spelling over
    // the C boundary, matching SmolVMHostMount and published socket paths.
    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encodeIfPresent(image, forKey: .image)
        try container.encode(mounts, forKey: .mounts)
        try container.encode(ports, forKey: .ports)
        try container.encode(publishedSockets, forKey: .publishedSockets)
        try container.encodeIfPresent(dockerSocket?.path, forKey: .dockerSocket)
        try container.encode(initCommands, forKey: .initCommands)
        try container.encodeIfPresent(resources, forKey: .resources)
        try container.encode(persistent, forKey: .persistent)
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let dockerSocketPath = try container.decodeIfPresent(String.self, forKey: .dockerSocket)
        self.init(
            name: try container.decode(String.self, forKey: .name),
            image: try container.decodeIfPresent(String.self, forKey: .image),
            mounts: try container.decode([SmolVMHostMount].self, forKey: .mounts),
            ports: try container.decode([SmolVMPortMapping].self, forKey: .ports),
            publishedSockets: try container.decode([SmolVMPublishedSocket].self, forKey: .publishedSockets),
            dockerSocket: dockerSocketPath.map { URL(fileURLWithPath: $0) },
            initCommands: try container.decode([String].self, forKey: .initCommands),
            resources: try container.decodeIfPresent(SmolVMResources.self, forKey: .resources),
            persistent: try container.decode(Bool.self, forKey: .persistent)
        )
    }

    private enum CodingKeys: String, CodingKey {
        case name, image, mounts, ports, publishedSockets, dockerSocket
        case initCommands, resources, persistent
    }
}

public struct SmolVMEnvironmentEntry: Codable, Sendable, Equatable {
    public let key: String
    public let value: String

    public init(key: String, value: String) {
        self.key = key
        self.value = value
    }
}

public struct SmolVMExecutionSpecification: Codable, Sendable, Equatable {
    public let command: [String]
    public let environment: [SmolVMEnvironmentEntry]
    public let workingDirectory: String?
    public let timeoutSeconds: UInt64?

    public init(
        command: [String],
        environment: [SmolVMEnvironmentEntry] = [],
        workingDirectory: String? = nil,
        timeoutSeconds: UInt64? = nil
    ) {
        self.command = command
        self.environment = environment
        self.workingDirectory = workingDirectory
        self.timeoutSeconds = timeoutSeconds
    }
}

public enum SmolVMOutputEvent: Sendable, Equatable {
    case stdout(Data)
    case stderr(Data)
}

public struct SmolVMMachineStatus: Codable, Sendable, Equatable {
    public let abiVersion: UInt32
    public let state: String
    public let running: Bool
    public let pid: Int32?
}

/// OCI metadata returned by smolvm's in-guest image store.
public struct SmolVMImageInfo: Codable, Sendable, Equatable {
    public let reference: String
    public let digest: String
    public let size: UInt64
    public let created: String?
    public let architecture: String
    public let os: String
    public let layerCount: Int
    public let layers: [String]
    public let entrypoint: [String]
    public let cmd: [String]
    public let env: [String]
    public let workdir: String?
    public let user: String?
}

/// Thread-safe owner of one smolvm embedded runtime.
///
/// Calls that can block in the guest are made by `SmolVMExecution`'s dedicated
/// worker thread. The SDK never launches the `smolvm` CLI or contacts its
/// daemon; it calls only the versioned C ABI in `smolvm-swift-ffi`.
public final class SmolVMRuntime: @unchecked Sendable {
    private let native: NativeLibrary
    private let runtime: UnsafeMutableRawPointer

    public init(configuration: SmolVMRuntimeConfiguration) throws {
        let loadedNative = try NativeLibrary(url: configuration.nativeLibrary)
        native = loadedNative
        let options = RuntimeOptions(
            stateDirectory: configuration.stateDirectory.path,
            libDirectory: configuration.libDirectory.path,
            agentRootfs: configuration.agentRootfs.path,
            bootBinary: configuration.bootHelper.path
        )
        let data = try JSONEncoder().encode(options)
        let json = String(decoding: data, as: UTF8.self)
        runtime = try json.withCString { optionsJSON in
            var error: UnsafeMutablePointer<CChar>?
            let created = loadedNative.runtimeCreate(optionsJSON, &error)
            return try loadedNative.requireValue(created, error: &error)
        }
    }

    deinit {
        native.runtimeFree(runtime)
    }

    /// Opens a persistent machine, creating its record if absent. Names must be
    /// private to the calling application: an existing record is deliberately
    /// reconnected rather than overwritten.
    public func openOrCreateMachine(
        _ specification: SmolVMMachineSpecification
    ) throws -> SmolVMMachine {
        let json = try Self.encode(specification)
        try json.withCString { request in
            try native.status { error in
                native.machineCreate(runtime, request, error)
            }
        }
        return SmolVMMachine(runtime: self, name: specification.name)
    }

    fileprivate func startMachine(named name: String) throws {
        try namedAction(name, native.machineStart)
    }

    fileprivate func stopMachine(named name: String) throws {
        try namedAction(name, native.machineStop)
    }

    fileprivate func startImageWorkload(named name: String) throws {
        try namedAction(name, native.machineStartImageWorkload)
    }

    fileprivate func deleteMachine(named name: String) throws {
        try namedAction(name, native.machineDelete)
    }

    fileprivate func status(of name: String) throws -> SmolVMMachineStatus {
        try name.withCString { rawName in
            try native.json { error in
                native.machineStatus(runtime, rawName, error)
            }
        }
    }

    fileprivate func startStream(
        named name: String,
        specification: SmolVMExecutionSpecification
    ) throws -> UnsafeMutableRawPointer {
        let json = try Self.encode(specification)
        return try name.withCString { rawName in
            try json.withCString { rawSpecification in
                var error: UnsafeMutablePointer<CChar>?
                let stream = native.streamStart(runtime, rawName, rawSpecification, &error)
                return try native.requireValue(stream, error: &error)
            }
        }
    }

    fileprivate func nextStreamEvent(
        _ stream: UnsafeMutableRawPointer,
        timeoutMilliseconds: UInt64
    ) throws -> NativeStreamEvent {
        try native.json { error in
            native.streamNext(stream, timeoutMilliseconds, error)
        }
    }

    fileprivate func freeStream(_ stream: UnsafeMutableRawPointer) {
        native.streamFree(stream)
    }

    fileprivate func pullImage(
        named name: String,
        reference: String
    ) throws -> SmolVMImageInfo {
        try name.withCString { rawName in
            try reference.withCString { rawReference in
                try native.json { error in
                    native.imagePull(runtime, rawName, rawReference, error)
                }
            }
        }
    }

    fileprivate func listImages(named name: String) throws -> [SmolVMImageInfo] {
        try name.withCString { rawName in
            try native.json { error in
                native.imageList(runtime, rawName, error)
            }
        }
    }

    private func namedAction(_ name: String, _ action: NativeLibrary.MachineAction) throws {
        try name.withCString { rawName in
            try native.status { error in
                action(runtime, rawName, error)
            }
        }
    }

    private static func encode<T: Encodable>(_ value: T) throws -> String {
        String(decoding: try JSONEncoder().encode(value), as: UTF8.self)
    }
}

public final class SmolVMMachine: @unchecked Sendable {
    private let runtime: SmolVMRuntime
    public let name: String

    fileprivate init(runtime: SmolVMRuntime, name: String) {
        self.runtime = runtime
        self.name = name
    }

    public func start() throws {
        try runtime.startMachine(named: name)
    }

    /// Stops by terminating the VM itself, including an exec currently running
    /// inside it. This is intentionally not a best-effort host-side process
    /// kill; the native smolvm runtime updates its durable machine state.
    public func stop() throws {
        try runtime.stopMachine(named: name)
    }

    /// Starts the image-defined ENTRYPOINT/CMD as a durable container. Normal
    /// container-engine callers use `prepareExecution`; service adapters use
    /// this explicit operation when the image itself owns the long-running
    /// process, such as `buildkitd`.
    public func startImageWorkload() throws {
        try runtime.startImageWorkload(named: name)
    }

    public func delete() throws {
        try runtime.deleteMachine(named: name)
    }

    public func status() throws -> SmolVMMachineStatus {
        try runtime.status(of: name)
    }

    public func prepareExecution(
        _ specification: SmolVMExecutionSpecification
    ) -> SmolVMExecution {
        SmolVMExecution(runtime: runtime, machineName: name, specification: specification)
    }

    public func pullImage(_ reference: String) throws -> SmolVMImageInfo {
        try runtime.pullImage(named: name, reference: reference)
    }

    public func listImages() throws -> [SmolVMImageInfo] {
        try runtime.listImages(named: name)
    }
}

/// One streaming, non-interactive smolvm guest exec.
public final class SmolVMExecution: @unchecked Sendable {
    private let runtime: SmolVMRuntime
    private let machineName: String
    private let specification: SmolVMExecutionSpecification
    private let lock = NSLock()
    private let outputRelay = OutputRelay()
    private var task: Task<Int32, Error>?

    fileprivate init(
        runtime: SmolVMRuntime,
        machineName: String,
        specification: SmolVMExecutionSpecification
    ) {
        self.runtime = runtime
        self.machineName = machineName
        self.specification = specification
    }

    public func start() throws {
        lock.lock()
        defer { lock.unlock() }
        guard task == nil else { return }
        let stream = try runtime.startStream(named: machineName, specification: specification)
        let relay = outputRelay
        let runtime = runtime
        task = Task.detached {
            defer { runtime.freeStream(stream) }
            while true {
                let event = try runtime.nextStreamEvent(stream, timeoutMilliseconds: 100)
                switch event.kind {
                case "stdout":
                    guard let data = event.data else {
                        throw SmolVMError.invalidResponse("stdout event without bytes")
                    }
                    relay.append(.stdout(data))
                case "stderr":
                    guard let data = event.data else {
                        throw SmolVMError.invalidResponse("stderr event without bytes")
                    }
                    relay.append(.stderr(data))
                case "exit":
                    guard let code = event.exitCode else {
                        throw SmolVMError.invalidResponse("exit event without status")
                    }
                    relay.finish()
                    return code
                case "error":
                    throw SmolVMError.nativeFailure(event.message ?? "unknown guest exec error")
                case "pending":
                    continue
                case "finished":
                    throw SmolVMError.executionFinishedWithoutExit
                default:
                    throw SmolVMError.invalidResponse("unknown stream event \(event.kind)")
                }
            }
        }
        let completion = task
        Task.detached {
            do {
                _ = try await completion?.value
            } catch {
                relay.fail(error)
            }
        }
    }

    public func wait() async throws -> Int32 {
        let task = lock.withLock { self.task }
        guard let task else { throw SmolVMError.executionNotStarted }
        return try await task.value
    }

    public func output() throws -> AsyncThrowingStream<SmolVMOutputEvent, Error> {
        guard lock.withLock({ task != nil }) else {
            throw SmolVMError.executionNotStarted
        }
        return outputRelay.stream()
    }
}

private struct RuntimeOptions: Encodable {
    let stateDirectory: String
    let libDirectory: String
    let agentRootfs: String
    let bootBinary: String
}

private struct NativeStreamEvent: Decodable {
    let abiVersion: UInt32
    let kind: String
    let dataBase64: String?
    let exitCode: Int32?
    let message: String?

    var data: Data? {
        guard let dataBase64 else { return nil }
        return Data(base64Encoded: dataBase64)
    }
}

private final class OutputRelay: @unchecked Sendable {
    private let lock = NSLock()
    private var buffered: [SmolVMOutputEvent] = []
    private var continuations: [UUID: AsyncThrowingStream<SmolVMOutputEvent, Error>.Continuation] = [:]
    private var finished = false
    private var failure: Error?

    func append(_ event: SmolVMOutputEvent) {
        let continuations = lock.withLock { () -> [AsyncThrowingStream<SmolVMOutputEvent, Error>.Continuation] in
            buffered.append(event)
            return Array(self.continuations.values)
        }
        for continuation in continuations {
            continuation.yield(event)
        }
    }

    func stream() -> AsyncThrowingStream<SmolVMOutputEvent, Error> {
        let identifier = UUID()
        return AsyncThrowingStream { continuation in
            let result = lock.withLock { () -> (Bool, Error?) in
                for event in buffered {
                    continuation.yield(event)
                }
                if !finished {
                    continuations[identifier] = continuation
                }
                return (finished, failure)
            }
            if result.0 {
                continuation.finish(throwing: result.1)
            }
            continuation.onTermination = { [weak self] _ in
                _ = self?.lock.withLock {
                    self?.continuations.removeValue(forKey: identifier)
                }
            }
        }
    }

    func finish() {
        let continuations = lock.withLock { () -> [AsyncThrowingStream<SmolVMOutputEvent, Error>.Continuation] in
            finished = true
            defer { self.continuations.removeAll() }
            return Array(self.continuations.values)
        }
        for continuation in continuations {
            continuation.finish()
        }
    }

    func fail(_ error: Error) {
        let continuations = lock.withLock { () -> [AsyncThrowingStream<SmolVMOutputEvent, Error>.Continuation] in
            failure = error
            finished = true
            defer { self.continuations.removeAll() }
            return Array(self.continuations.values)
        }
        for continuation in continuations {
            continuation.finish(throwing: error)
        }
    }
}

private final class NativeLibrary: @unchecked Sendable {
    typealias RuntimeCreate = @convention(c) (
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> UnsafeMutableRawPointer?
    typealias RuntimeFree = @convention(c) (UnsafeMutableRawPointer?) -> Void
    typealias MachineAction = @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> Int32
    typealias MachineStatus = @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> UnsafeMutablePointer<CChar>?
    typealias StreamStart = @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> UnsafeMutableRawPointer?
    typealias StreamNext = @convention(c) (
        UnsafeMutableRawPointer?,
        UInt64,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> UnsafeMutablePointer<CChar>?
    typealias StreamFree = @convention(c) (UnsafeMutableRawPointer?) -> Void
    typealias ImagePull = @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafePointer<CChar>?,
        UnsafePointer<CChar>?,
        UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
    ) -> UnsafeMutablePointer<CChar>?
    typealias StringFree = @convention(c) (UnsafeMutablePointer<CChar>?) -> Void

    let handle: UnsafeMutableRawPointer
    let runtimeCreate: RuntimeCreate
    let runtimeFree: RuntimeFree
    let machineCreate: MachineAction
    let machineStart: MachineAction
    let machineStartImageWorkload: MachineAction
    let machineStop: MachineAction
    let machineDelete: MachineAction
    let machineStatus: MachineStatus
    let streamStart: StreamStart
    let streamNext: StreamNext
    let streamFree: StreamFree
    let imagePull: ImagePull
    let imageList: MachineStatus
    private let stringFree: StringFree

    init(url: URL) throws {
        guard let handle = dlopen(url.path, RTLD_NOW | RTLD_LOCAL) else {
            let reason = dlerror().map { String(cString: $0) } ?? "unknown dynamic-loader error"
            throw SmolVMError.nativeLibraryUnavailable(url, reason)
        }
        self.handle = handle
        do {
            runtimeCreate = try Self.load("smolvm_swift_runtime_create", from: handle)
            runtimeFree = try Self.load("smolvm_swift_runtime_free", from: handle)
            machineCreate = try Self.load("smolvm_swift_machine_create", from: handle)
            machineStart = try Self.load("smolvm_swift_machine_start", from: handle)
            machineStartImageWorkload = try Self.load(
                "smolvm_swift_machine_start_image_workload",
                from: handle
            )
            machineStop = try Self.load("smolvm_swift_machine_stop", from: handle)
            machineDelete = try Self.load("smolvm_swift_machine_delete", from: handle)
            machineStatus = try Self.load("smolvm_swift_machine_status", from: handle)
            streamStart = try Self.load("smolvm_swift_machine_exec_stream_start", from: handle)
            streamNext = try Self.load("smolvm_swift_stream_next", from: handle)
            streamFree = try Self.load("smolvm_swift_stream_free", from: handle)
            imagePull = try Self.load("smolvm_swift_image_pull", from: handle)
            imageList = try Self.load("smolvm_swift_image_list", from: handle)
            stringFree = try Self.load("smolvm_swift_string_free", from: handle)
        } catch {
            dlclose(handle)
            throw error
        }
    }

    deinit {
        dlclose(handle)
    }

    func status(_ invoke: (UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> Int32) throws {
        var error: UnsafeMutablePointer<CChar>?
        let result = invoke(&error)
        defer { free(error) }
        guard result == 0 else {
            throw nativeError(error)
        }
    }

    func json<T: Decodable>(
        _ invoke: (UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) throws -> T {
        var error: UnsafeMutablePointer<CChar>?
        let result = invoke(&error)
        defer {
            free(result)
            free(error)
        }
        guard let result else {
            throw nativeError(error)
        }
        do {
            return try JSONDecoder().decode(T.self, from: Data(String(cString: result).utf8))
        } catch {
            throw SmolVMError.invalidResponse(error.localizedDescription)
        }
    }

    func requireValue(
        _ value: UnsafeMutableRawPointer?,
        error: inout UnsafeMutablePointer<CChar>?
    ) throws -> UnsafeMutableRawPointer {
        defer { free(error) }
        guard let value else {
            throw nativeError(error)
        }
        return value
    }

    private func nativeError(_ pointer: UnsafeMutablePointer<CChar>?) -> SmolVMError {
        guard let pointer else {
            return .nativeFailure("smolvm native library returned no error detail")
        }
        if let body = try? JSONDecoder().decode(NativeError.self, from: Data(String(cString: pointer).utf8)) {
            return .nativeFailure(body.message)
        }
        return .nativeFailure(String(cString: pointer))
    }

    private func free(_ pointer: UnsafeMutablePointer<CChar>?) {
        if let pointer {
            stringFree(pointer)
        }
    }

    private static func load<T>(_ name: String, from handle: UnsafeMutableRawPointer) throws -> T {
        guard let symbol = dlsym(handle, name) else {
            throw SmolVMError.nativeSymbolMissing(name)
        }
        return unsafeBitCast(symbol, to: T.self)
    }
}

private struct NativeError: Decodable {
    let abiVersion: UInt32
    let message: String
}
