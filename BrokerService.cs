using Broker;
using Grpc.Core;
using System.Collections.Concurrent;

public class BrokerService : BrokerProto.BrokerProtoBase
{
    private static readonly ConcurrentDictionary<string, IServerStreamWriter<BrokerMessage>> _connectedClients = new();
    private static readonly ConcurrentDictionary<string, TaskCompletionSource<FileResponse>> _pendingResponses = new();

    // Method to handle client connection and messages
    public override async Task ConnectClient(IAsyncStreamReader<ClientMessage> requestStream,
                                             IServerStreamWriter<BrokerMessage> responseStream,
                                             ServerCallContext context)
    {
        await foreach (var clientMessage in requestStream.ReadAllAsync())
        {
            // Add client to the dictionary when they first connect
            if (!_connectedClients.ContainsKey(clientMessage.ClientId))
            {
                Console.WriteLine($"Client connected: {clientMessage.ClientId}");
                _connectedClients[clientMessage.ClientId] = responseStream;
            }

            // Handle Heartbeat
            if (clientMessage.Heartbeat != null)
            {
                Console.WriteLine($"Heartbeat from {clientMessage.ClientId}");
            }

            // Handle FileResponse
            if (clientMessage.FileResponse != null)
            {
                Console.WriteLine($"Response from {clientMessage.ClientId}: {clientMessage.FileResponse.Message}");

                if (_pendingResponses.TryRemove(clientMessage.ClientId, out var tcs))
                {
                    tcs.SetResult(clientMessage.FileResponse);
                }
            }
        }

        // Cleanup when the connection ends
        Console.WriteLine($"Client disconnected: {context.Peer}");
        _connectedClients.TryRemove(context.Peer, out _);
    }

    // Method to forward command to the client
    public override async Task<ClientResponse> ForwardToClient(ClientCommand request, ServerCallContext context)
    {
        // Try to get the client's stream using their ClientId
        if (_connectedClients.TryGetValue(request.ClientId, out var clientStream))
        {
            // Create the broker message
            var brokerMessage = new BrokerMessage
            {
                FileCommand = request.Command
            };

            var tcs = new TaskCompletionSource<FileResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingResponses[request.ClientId] = tcs;

            // Send the message to the connected client
            await clientStream.WriteAsync(brokerMessage);
            Console.WriteLine($"Command sent to {request.ClientId}, waiting for response...");

            var timeout = Task.Delay(TimeSpan.FromSeconds(10)); // optional timeout
            var completedTask = await Task.WhenAny(tcs.Task, timeout);

            if (completedTask == timeout)
            {
                _pendingResponses.TryRemove(request.ClientId, out _);
                return new ClientResponse
                {
                    ClientId = request.ClientId,
                    Response = new FileResponse
                    {
                        Success = false,
                        Message = "Client did not respond in time"
                    }
                };
            }

            var fileResponse = await tcs.Task;
            return new ClientResponse
            {
                ClientId = request.ClientId,
                Response = fileResponse
            };
        }

        // If client is not connected, return failure response
        return new ClientResponse
        {
            ClientId = request.ClientId,
            Response = new FileResponse
            {
                Success = false,
                Message = "Client not connected"
            }
        };
    }
}
