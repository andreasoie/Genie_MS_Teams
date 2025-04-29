"""
Databricks Genie Bot

Author: Luiz Carrossoni Neto
Revision: 1.0

This script implements an experimental chatbot that interacts with Databricks' Genie API,
which is currently in Private Preview. The bot facilitates conversations with Genie,
Databricks' AI assistant, through a chat interface.

Note: This is experimental code and is not intended for production use.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

from aiohttp import web
from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.core.message_factory import MessageFactory
from botbuilder.schema import Activity, ActivityTypes, ChannelAccount
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()

# Load required environment variables
DATABRICKS_SPACE_ID = os.getenv("DATABRICKS_SPACE_ID")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MICROSOFT_APP_ID = os.getenv("MICROSOFT_APP_ID", "")
MICROSOFT_APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")

# Validate required environment variables
if not DATABRICKS_SPACE_ID:
    raise ValueError("DATABRICKS_SPACE_ID environment variable is required")
if not DATABRICKS_HOST:
    raise ValueError("DATABRICKS_HOST environment variable is required")
if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN environment variable is required")

logger.info("Environment variables validated successfully")
logger.info(f"Initializing Databricks client with host: {DATABRICKS_HOST}")
logger.info(f"Using Space ID: {DATABRICKS_SPACE_ID}")
logger.info("Attempting to create Databricks workspace client...")

workspace_client = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN,
    auth_type="pat",  # Explicitly specify Personal Access Token authentication
)
genie_api = GenieAPI(workspace_client.api_client)


async def ask_genie(
    question: str, space_id: str, conversation_id: Optional[str] = None
) -> tuple[str, str]:
    try:
        loop = asyncio.get_running_loop()
        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )
            conversation_id = initial_message.conversation_id
        else:
            initial_message = await loop.run_in_executor(
                None,
                genie_api.create_message_and_wait,
                space_id,
                conversation_id,
                question,
            )

        query_result = None
        if initial_message.query_result is not None:
            query_result = await loop.run_in_executor(
                None,
                genie_api.get_message_query_result,
                space_id,
                initial_message.conversation_id,
                initial_message.id,
            )

        message_content = await loop.run_in_executor(
            None,
            genie_api.get_message,
            space_id,
            initial_message.conversation_id,
            initial_message.id,
        )

        if query_result and query_result.statement_response:
            results = await loop.run_in_executor(
                None,
                workspace_client.statement_execution.get_statement,
                query_result.statement_response.statement_id,
            )

            query_description = ""
            for attachment in message_content.attachments:
                if attachment.query and attachment.query.description:
                    query_description = attachment.query.description
                    break

            return json.dumps(
                {
                    "columns": results.manifest.schema.as_dict(),
                    "data": results.result.as_dict(),
                    "query_description": query_description,
                }
            ), conversation_id

        if message_content.attachments:
            for attachment in message_content.attachments:
                if attachment.text and attachment.text.content:
                    return json.dumps(
                        {"message": attachment.text.content}
                    ), conversation_id

        return json.dumps({"message": message_content.content}), conversation_id
    except Exception as e:
        logger.error(f"Error in ask_genie: {str(e)}")
        return json.dumps(
            {"error": "An error occurred while processing your request."}
        ), conversation_id


def process_query_results(answer_json: Dict) -> str:
    response = ""
    if "query_description" in answer_json and answer_json["query_description"]:
        response += f"## Query Description\n\n{answer_json['query_description']}\n\n"

    if "columns" in answer_json and "data" in answer_json:
        response += "## Query Results\n\n"
        columns = answer_json["columns"]
        data = answer_json["data"]
        if isinstance(columns, dict) and "columns" in columns:
            header = "| " + " | ".join(col["name"] for col in columns["columns"]) + " |"
            separator = "|" + "|".join(["---" for _ in columns["columns"]]) + "|"
            response += header + "\n" + separator + "\n"
            for row in data["data_array"]:
                formatted_row = []
                for value, col in zip(row, columns["columns"]):
                    if value is None:
                        formatted_value = "NULL"
                    elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                        formatted_value = f"{float(value):,.2f}"
                    elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                        formatted_value = f"{int(value):,}"
                    else:
                        formatted_value = str(value)
                    formatted_row.append(formatted_value)
                response += "| " + " | ".join(formatted_row) + " |\n"
        else:
            response += f"Unexpected column format: {columns}\n\n"
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "No data available.\n\n"

    return response


def process_for_slack(answer_json: Dict) -> List[Dict]:
    """
    Formats the response data into Slack Block Kit JSON.
    """
    blocks = []

    # Add Query Description if available
    if "query_description" in answer_json and answer_json["query_description"]:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Query Description:*\n{answer_json['query_description']}",
                },
            }
        )
        blocks.append({"type": "divider"})

    # Process Table Data
    if "columns" in answer_json and "data" in answer_json:
        columns = answer_json["columns"]
        data = answer_json["data"]

        if isinstance(columns, dict) and "columns" in columns and "data_array" in data:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Query Results:*"},
                }
            )

            # Prepare header and rows for mrkdwn table (using code block for alignment)
            header_names = [col["name"] for col in columns["columns"]]
            col_widths = [len(name) for name in header_names]
            formatted_rows = []

            for row_data in data["data_array"]:
                formatted_row = []
                for i, value in enumerate(row_data):
                    col = columns["columns"][i]
                    if value is None:
                        formatted_value = "NULL"
                    elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                        formatted_value = f"{float(value):,.2f}"
                    elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                        formatted_value = f"{int(value):,}"
                    else:
                        formatted_value = str(value)

                    # Update column width based on data
                    col_widths[i] = max(col_widths[i], len(formatted_value))
                    formatted_row.append(formatted_value)
                formatted_rows.append(formatted_row)

            # Build the table string with padding
            header_str = " | ".join(
                name.ljust(col_widths[i]) for i, name in enumerate(header_names)
            )
            separator_str = "-+-".join(
                "-" * width for width in col_widths
            )  # Use a simpler separator
            table_str = f"{header_str}\n{separator_str}\n"

            for row in formatted_rows:
                table_str += (
                    " | ".join(val.ljust(col_widths[i]) for i, val in enumerate(row))
                    + "\n"
                )

            # Add table in a code block for fixed-width font
            # Limit block size to Slack's 3000 char limit for text fields
            max_chars = 2950  # A bit less than 3000 for safety
            truncated_table_str = table_str[:max_chars] + (
                "..." if len(table_str) > max_chars else ""
            )

            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"```{truncated_table_str}```"},
                }
            )

        else:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Unexpected data format received.",
                    },
                }
            )

    # Process Simple Message
    elif "message" in answer_json:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": answer_json["message"]},
            }
        )
    # Handle No Data Case
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "No data available."},
            }
        )

    return blocks


SETTINGS = BotFrameworkAdapterSettings(MICROSOFT_APP_ID, MICROSOFT_APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)


class MyBot(ActivityHandler):
    def __init__(self):
        self.conversation_ids: Dict[str, str] = {}

    async def on_message_activity(self, turn_context: TurnContext):
        question = turn_context.activity.text
        user_id = turn_context.activity.from_property.id
        conversation_id = self.conversation_ids.get(user_id)

        try:
            answer, new_conversation_id = await ask_genie(
                question, DATABRICKS_SPACE_ID, conversation_id
            )
            self.conversation_ids[user_id] = new_conversation_id

            answer_json = json.loads(answer)

            channel = turn_context.activity.channel_id
            if channel == "slack":
                # Assuming process_for_slack returns Block Kit JSON list
                attachments = process_for_slack(answer_json)
                reply_activity = Activity(
                    type=ActivityTypes.message,
                    attachments=attachments,  # attachments is typically a list
                )
            # elif channel == "msteams":
            #     # Assuming process_for_teams returns an Adaptive Card JSON dict
            #     card = process_for_teams(answer_json)
            #     attachment = CardFactory.adaptive_card(card)
            #     reply_activity = MessageFactory.attachment(attachment)
            else:
                response_text = process_query_results(answer_json)
                reply_activity = MessageFactory.text(response_text)

            await turn_context.send_activity(reply_activity)
        except json.JSONDecodeError:
            await turn_context.send_activity(
                "Failed to decode response from the server."
            )
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await turn_context.send_activity(
                "An error occurred while processing your request."
            )

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Databricks Genie Bot!")


BOT = MyBot()


async def messages(req: web.Request) -> web.Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    try:
        response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
        if response:
            return web.json_response(data=response.body, status=response.status)
        return web.Response(status=201)
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return web.Response(status=500)


app = web.Application()
app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        host = os.getenv("HOST", "localhost")
        port = int(os.environ.get("PORT", 3978))
        web.run_app(app, host=host, port=port)
    except Exception:
        logger.exception("Error running app")
