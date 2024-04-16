using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EventHubConsumer;

class Program
{
    private const string ConnectionString = "Endpoint=sb://evhns-perfdata-prod.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=h8/LOLSrUyOdCU+piHR2sW6W/x9VtuudVewFhWqvkPs=;EntityPath=evh-externalrouting";
    private const string BlobConnectionString = "https://dlspsperfdataprod.blob.core.windows.net/checkpoints?sp=rawlo&st=2024-03-29T23:20:18Z&se=2024-04-30T07:20:18Z&spr=https&sv=2022-11-02&sr=c&sig=MWLybfOjJz6pVEfZBic3B3fR8zc0B16JCYySSqba8rc%3D";
    private static long messageCounter;
    private static long offset;
    private static long sequence;

    static async Task Main()
    {
        var blobClient = new BlobContainerClient(new Uri(BlobConnectionString));
        var eventProcessorClient = new EventProcessorClient(blobClient, EventHubConsumerClient.DefaultConsumerGroupName, ConnectionString);

        eventProcessorClient.ProcessEventAsync += ProcessEventHandler;
        eventProcessorClient.ProcessErrorAsync += ProcessErrorHandler;

        await eventProcessorClient.StartProcessingAsync();

        Console.WriteLine("Press any key to stop...");
        Console.ReadKey();

        await eventProcessorClient.StopProcessingAsync();
    }

    private static Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        var data = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
        Console.WriteLine($"Received event: {data}");
        //Handle the data, this is the reporting payload

        messageCounter++;
        
        if (eventArgs.Data.Offset > offset)
            offset = eventArgs.Data.Offset;

        if(eventArgs.Data.SequenceNumber > sequence)
            sequence = eventArgs.Data.SequenceNumber;

        //checkpoint the message after consuming 500, can also add a timer to checkpoint
        if (messageCounter % 500 == 0)
        {
            eventArgs.UpdateCheckpointAsync();
            Console.WriteLine($"Processed {messageCounter} messages");
            Console.WriteLine($"Offset: {offset}");
            Console.WriteLine($"Sequence: {sequence}");
            Console.WriteLine($"Part: {eventArgs.Partition.PartitionId}");
        }

        return Task.CompletedTask;
    }

    private static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        Console.WriteLine($"Error occurred: {eventArgs.Exception.Message}");
        return Task.CompletedTask;
    }
}