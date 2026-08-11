import Foundation
@testable import SmolVMSDK
import Testing

@Test("runtime configuration preserves explicit asset and state paths")
func runtimeConfigurationNormalizesPaths() {
    let root = URL(fileURLWithPath: "/tmp/smolvm-sdk-test", isDirectory: true)
    let configuration = SmolVMRuntimeConfiguration(
        nativeLibrary: root.appendingPathComponent("ffi.dylib"),
        stateDirectory: root.appendingPathComponent("state/../state", isDirectory: true),
        libDirectory: root.appendingPathComponent("lib", isDirectory: true),
        agentRootfs: root.appendingPathComponent("rootfs", isDirectory: true),
        bootHelper: root.appendingPathComponent("smolvm-boot")
    )

    #expect(configuration.stateDirectory.path == "/tmp/smolvm-sdk-test/state")
    #expect(configuration.nativeLibrary.path == "/tmp/smolvm-sdk-test/ffi.dylib")
    #expect(configuration.bootHelper.path == "/tmp/smolvm-sdk-test/smolvm-boot")
}

@Test("machine specification carries persistent Docker socket setup")
func machineSpecificationCarriesDockerSocketSetup() throws {
    let specification = SmolVMMachineSpecification(
        name: "docker-host",
        image: nil,
        publishedSockets: [
            SmolVMPublishedSocket(
                direction: .expose,
                guestPath: "/var/run/docker.sock",
                hostPath: "/tmp/docker-host.sock"
            ),
        ],
        dockerSocket: URL(fileURLWithPath: "/tmp/docker-host.sock"),
        initCommands: [
            "apk add --no-cache docker",
            "dockerd --data-root=/storage/docker &",
        ],
        resources: SmolVMResources(network: true, storageGiB: 20, overlayGiB: 10),
        persistent: true
    )

    let object = try JSONSerialization.jsonObject(
        with: JSONEncoder().encode(specification),
        options: []
    ) as? [String: Any]
    #expect(object?["persistent"] as? Bool == true)
    #expect(object?["initCommands"] as? [String] == specification.initCommands)
    #expect(object?["dockerSocket"] as? String == "/tmp/docker-host.sock")
    #expect(object?["publishedSockets"] != nil)
    #expect(object?["resources"] != nil)

    let roundTrip = try JSONDecoder().decode(
        SmolVMMachineSpecification.self,
        from: JSONEncoder().encode(specification)
    )
    #expect(roundTrip == specification)
}
