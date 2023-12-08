from typing import List
import json
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import DefaultAzureCredential


async def send(
    sb_name: str,
    queue_name: str,
    type: str,
    credential: DefaultAzureCredential,
    messages: List[dict],
):
    """Send messages to the Service Bus queue."""
    async with ServiceBusClient(
        fully_qualified_namespace=f"sb://{sb_name}.servicebus.windows.net",
        credential=credential,
        logging_enable=True,
    ) as servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name=queue_name)
        async with sender:
            # works with Python>=3.10
            # match type:
            #     case "single":
            #         await send_single_message(sender, messages[0])
            #     case "list":
            #         await send_a_list_of_messages(sender, messages)
            #     case "batch":
            #         await send_batch_message(sender, messages)
            #     case _:
            #         raise ValueError("Invalid type")
            # works with Python<3.10
            if type == "single":
                await send_single_message(sender, messages[0])
            elif type == "list":
                await send_a_list_of_messages(sender, messages)
            elif type == "batch":
                await send_batch_message(sender, messages)
            else:
                raise ValueError("Invalid type")
        await credential.close()


async def send_single_message(sender, single_message: dict):
    """Create a Service Bus message and send it to the queue"""
    message = ServiceBusMessage(json.dumps(single_message))
    await sender.send_messages(message)


async def send_a_list_of_messages(sender, message_list: List[dict]):
    """Create a list of messages and send it to the queue"""
    messages = [ServiceBusMessage(json.dumps(message)) for message in message_list]
    await sender.send_messages(messages)


async def send_batch_message(sender, message_list: List[dict]):
    """Create a batch of messages"""
    async with sender:
        batch_message = await sender.create_message_batch()
        for message in message_list:
            try:
                batch_message.add_message(ServiceBusMessage(json.dumps(message)))
            except ValueError:
                # ServiceBusMessageBatch object reaches max_size.
                # New ServiceBusMessageBatch object can be created here to send more data.
                break
        await sender.send_messages(batch_message)
