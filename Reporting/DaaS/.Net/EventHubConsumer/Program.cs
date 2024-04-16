using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

namespace EventHubConsumer;

class Program
{
    private const string ConnectionString = "<InsertAzureEventHubConnectionStingHere>";
    private const string BlobConnectionString = "<InsertAzureBlobStorageConnectionStringHere>";
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