#!/usr/bin/env python
"""
Google Spreadsheet MCP Server
A Model Context Protocol (MCP) server built with FastMCP for interacting with Google Sheets.
"""

import base64
import os
import sys
from typing import List, Dict, Any, Optional, Union
import json
from dataclasses import dataclass
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

# MCP imports
from mcp.server.fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations

# Google API imports
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_CONFIG = os.environ.get('CREDENTIALS_CONFIG')
TOKEN_PATH = os.environ.get('TOKEN_PATH', 'token.json')
CREDENTIALS_PATH = os.environ.get('CREDENTIALS_PATH', 'credentials.json')
SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_PATH', 'service_account.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')  # Working directory in Google Drive

# Tool filtering configuration
# Parse enabled tools from environment variable or command-line argument
def _parse_enabled_tools() -> Optional[set]:
    """
    Parse enabled tools from ENABLED_TOOLS environment variable or --include-tools argument.
    Returns None if all tools should be enabled (default behavior).
    Returns a set of tool names if filtering is requested.
    """
    # Check command-line arguments first
    enabled_tools_str = None
    for i, arg in enumerate(sys.argv):
        if arg == '--include-tools' and i + 1 < len(sys.argv):
            enabled_tools_str = sys.argv[i + 1]
            break
    
    # Fall back to environment variable
    if not enabled_tools_str:
        enabled_tools_str = os.environ.get('ENABLED_TOOLS')
    
    if not enabled_tools_str:
        return None  # No filtering, enable all tools
    
    # Parse comma-separated list and normalize
    tools = {tool.strip() for tool in enabled_tools_str.split(',') if tool.strip()}
    return tools if tools else None

ENABLED_TOOLS = _parse_enabled_tools()

@dataclass
class SpreadsheetContext:
    """Context for Google Spreadsheet service"""
    sheets_service: Any
    drive_service: Any
    folder_id: Optional[str] = None


@asynccontextmanager
async def spreadsheet_lifespan(server: FastMCP) -> AsyncIterator[SpreadsheetContext]:
    """Manage Google Spreadsheet API connection lifecycle"""
    # Authenticate and build the service
    creds = None

    if CREDENTIALS_CONFIG:
        creds = service_account.Credentials.from_service_account_info(json.loads(base64.b64decode(CREDENTIALS_CONFIG)), scopes=SCOPES)
    
    # Check for explicit service account authentication first (custom SERVICE_ACCOUNT_PATH)
    if not creds and SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
        try:
            # Regular service account authentication
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_PATH,
                scopes=SCOPES
            )
            print("Using service account authentication")
            print(f"Working with Google Drive folder ID: {DRIVE_FOLDER_ID or 'Not specified'}")
        except Exception as e:
            print(f"Error using service account authentication: {e}")
            creds = None
    
    # Fall back to OAuth flow if service account auth failed or not configured
    if not creds:
        print("Trying OAuth authentication flow")
        if os.path.exists(TOKEN_PATH):
            with open(TOKEN_PATH, 'r') as token:
                creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)
                
        # If credentials are not valid or don't exist, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    print("Attempting to refresh expired token...")
                    creds.refresh(Request())
                    print("Token refreshed successfully")
                    # Save the refreshed token
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                except Exception as refresh_error:
                    print(f"Token refresh failed: {refresh_error}")
                    print("Triggering reauthentication flow...")
                    creds = None  # Clear creds to trigger OAuth flow below

            # If refresh failed or creds don't exist, run OAuth flow
            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
                    creds = flow.run_local_server(port=0)

                    # Save the credentials for the next run
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                    print("Successfully authenticated using OAuth flow")
                except Exception as e:
                    print(f"Error with OAuth flow: {e}")
                    creds = None
    
    # Try Application Default Credentials if no creds thus far
    # This will automatically check GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service
    if not creds:
        try:
            print("Attempting to use Application Default Credentials (ADC)")
            print("ADC will check: GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service")
            creds, project = google.auth.default(
                scopes=SCOPES
            )
            print(f"Successfully authenticated using ADC for project: {project}")
        except Exception as e:
            print(f"Error using Application Default Credentials: {e}")
            raise Exception("All authentication methods failed. Please configure credentials.")
    
    # Build the services
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    
    try:
        # Provide the service in the context
        yield SpreadsheetContext(
            sheets_service=sheets_service,
            drive_service=drive_service,
            folder_id=DRIVE_FOLDER_ID if DRIVE_FOLDER_ID else None
        )
    finally:
        # No explicit cleanup needed for Google APIs
        pass


# Initialize the MCP server with lifespan management
# Resolve host/port from environment variables with flexible names
_resolved_host = os.environ.get('HOST') or os.environ.get('FASTMCP_HOST') or "0.0.0.0"
_resolved_port_str = os.environ.get('PORT') or os.environ.get('FASTMCP_PORT') or "8000"
try:
    _resolved_port = int(_resolved_port_str)
except ValueError:
    _resolved_port = 8000

# Initialize the MCP server with explicit host/port to ensure binding as configured
mcp = FastMCP("Google Spreadsheet",
              dependencies=["google-auth", "google-auth-oauthlib", "google-api-python-client"],
              lifespan=spreadsheet_lifespan,
              host=_resolved_host,
              port=_resolved_port,
              stateless_http=True)


def tool(annotations: Optional[ToolAnnotations] = None):
    """
    Conditional tool decorator that only registers tools if they're enabled.
    
    This wrapper checks ENABLED_TOOLS configuration and only applies the @mcp.tool
    decorator if the tool should be enabled. If ENABLED_TOOLS is None (default),
    all tools are enabled.
    
    Args:
        annotations: Optional ToolAnnotations for the tool
    
    Returns:
        Decorator function
    """
    def decorator(func):
        tool_name = func.__name__
        
        # If no filtering is configured, or if this tool is in the enabled list
        if ENABLED_TOOLS is None or tool_name in ENABLED_TOOLS:
            # Apply the mcp.tool decorator
            if annotations:
                return mcp.tool(annotations=annotations)(func)
            else:
                return mcp.tool()(func)
        else:
            # Don't register this tool - return the function undecorated
            return func
    
    return decorator


@tool(
    annotations=ToolAnnotations(
        title="Get Sheet Data",
        readOnlyHint=True,
    ),
)
def get_sheet_data(spreadsheet_id: str,
                   sheet: str,
                   range: Optional[str] = None,
                   include_grid_data: bool = False,
                   row_offset: int = 0,
                   row_limit: Optional[int] = None,
                   ctx: Context = None) -> Dict[str, Any]:
    """
    Get data from a specific sheet in a Google Spreadsheet.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all data.
        include_grid_data: If True, includes cell formatting and other metadata in the response.
            Note: Setting this to True will significantly increase the response size and token usage.
            Default is False (returns values only, more efficient).
        row_offset: Number of rows to skip from the top (default 0). Use for pagination.
        row_limit: Maximum number of rows to return. If not set, returns all rows after offset.

    Returns:
        Grid data structure with values, plus pagination metadata (total_rows, row_offset, row_limit).
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet

    if include_grid_data:
        result = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            ranges=[full_range],
            includeGridData=True
        ).execute()
        return result

    values_result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=full_range
    ).execute()

    all_values = values_result.get('values', [])
    total_rows = len(all_values)

    paginated = all_values[row_offset:]
    if row_limit is not None:
        paginated = paginated[:row_limit]

    return {
        'spreadsheetId': spreadsheet_id,
        'valueRanges': [{
            'range': full_range,
            'values': paginated
        }],
        'pagination': {
            'total_rows': total_rows,
            'row_offset': row_offset,
            'row_limit': row_limit,
            'returned_rows': len(paginated),
            'has_more': (row_offset + len(paginated)) < total_rows,
        }
    }

@tool(
    annotations=ToolAnnotations(
        title="Get Sheet Formulas",
        readOnlyHint=True,
    ),
)
def get_sheet_formulas(spreadsheet_id: str,
                       sheet: str,
                       range: Optional[str] = None,
                       ctx: Context = None) -> List[List[Any]]:
    """
    Get formulas from a specific sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all formulas from the sheet.
    
    Returns:
        A 2D array of the sheet formulas.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Construct the range
    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet  # Get all formulas in the specified sheet
    
    # Call the Sheets API
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        valueRenderOption='FORMULA'  # Request formulas
    ).execute()
    
    # Get the formulas from the response
    formulas = result.get('values', [])
    return formulas

@tool(
    annotations=ToolAnnotations(
        title="List Sheets",
        readOnlyHint=True,
    ),
)
def list_sheets(spreadsheet_id: str, ctx: Context = None) -> List[str]:
    """
    List all sheets in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
    
    Returns:
        List of sheet names
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    # Extract sheet names
    sheet_names = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
    
    return sheet_names


@tool(
    annotations=ToolAnnotations(
        title="Get Multiple Sheet Data",
        readOnlyHint=True,
    ),
)
def get_multiple_sheet_data(queries: List[Dict[str, str]],
                            ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get data from multiple specific ranges in Google Spreadsheets.
    
    Args:
        queries: A list of dictionaries, each specifying a query. 
                 Each dictionary should have 'spreadsheet_id', 'sheet', and 'range' keys.
                 Example: [{'spreadsheet_id': 'abc', 'sheet': 'Sheet1', 'range': 'A1:B5'}, 
                           {'spreadsheet_id': 'xyz', 'sheet': 'Data', 'range': 'C1:C10'}]
    
    Returns:
        A list of dictionaries, each containing the original query parameters 
        and the fetched 'data' or an 'error'.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = []
    
    for query in queries:
        spreadsheet_id = query.get('spreadsheet_id')
        sheet = query.get('sheet')
        range_str = query.get('range')
        
        if not all([spreadsheet_id, sheet, range_str]):
            results.append({**query, 'error': 'Missing required keys (spreadsheet_id, sheet, range)'})
            continue

        try:
            # Construct the range
            full_range = f"{sheet}!{range_str}"
            
            # Call the Sheets API
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range
            ).execute()
            
            # Get the values from the response
            values = result.get('values', [])
            results.append({**query, 'data': values})

        except Exception as e:
            results.append({**query, 'error': str(e)})
            
    return results


@tool(
    annotations=ToolAnnotations(
        title="Get Multiple Spreadsheet Summary",
        readOnlyHint=True,
    ),
)
def get_multiple_spreadsheet_summary(spreadsheet_ids: List[str],
                                   rows_to_fetch: int = 5,
                                   ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get a summary of multiple Google Spreadsheets, including sheet names, 
    headers, and the first few rows of data for each sheet.
    
    Args:
        spreadsheet_ids: A list of spreadsheet IDs to summarize.
        rows_to_fetch: The number of rows (including header) to fetch for the summary (default: 5).
    
    Returns:
        A list of dictionaries, each representing a spreadsheet summary. 
        Includes spreadsheet title, sheet summaries (title, headers, first rows), or an error.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    summaries = []
    
    for spreadsheet_id in spreadsheet_ids:
        summary_data = {
            'spreadsheet_id': spreadsheet_id,
            'title': None,
            'sheets': [],
            'error': None
        }
        try:
            # Get spreadsheet metadata
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields='properties.title,sheets(properties(title,sheetId))'
            ).execute()
            
            summary_data['title'] = spreadsheet.get('properties', {}).get('title', 'Unknown Title')
            
            sheet_summaries = []
            for sheet in spreadsheet.get('sheets', []):
                sheet_title = sheet.get('properties', {}).get('title')
                sheet_id = sheet.get('properties', {}).get('sheetId')
                sheet_summary = {
                    'title': sheet_title,
                    'sheet_id': sheet_id,
                    'headers': [],
                    'first_rows': [],
                    'error': None
                }
                
                if not sheet_title:
                    sheet_summary['error'] = 'Sheet title not found'
                    sheet_summaries.append(sheet_summary)
                    continue
                    
                try:
                    # Fetch the first few rows (e.g., A1:Z5)
                    # Adjust range if fewer rows are requested
                    max_row = max(1, rows_to_fetch) # Ensure at least 1 row is fetched
                    range_to_get = f"{sheet_title}!A1:{max_row}" # Fetch all columns up to max_row
                    
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=range_to_get
                    ).execute()
                    
                    values = result.get('values', [])
                    
                    if values:
                        sheet_summary['headers'] = values[0]
                        if len(values) > 1:
                            sheet_summary['first_rows'] = values[1:max_row]
                    else:
                        # Handle empty sheets or sheets with less data than requested
                        sheet_summary['headers'] = []
                        sheet_summary['first_rows'] = []

                except Exception as sheet_e:
                    sheet_summary['error'] = f'Error fetching data for sheet {sheet_title}: {sheet_e}'
                
                sheet_summaries.append(sheet_summary)
            
            summary_data['sheets'] = sheet_summaries
            
        except Exception as e:
            summary_data['error'] = f'Error fetching spreadsheet {spreadsheet_id}: {e}'
            
        summaries.append(summary_data)
        
    return summaries


@mcp.resource("spreadsheet://{spreadsheet_id}/info")
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """
    Get basic information about a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
    
    Returns:
        JSON string with spreadsheet information
    """
    # Access the context through mcp.get_lifespan_context() for resources
    context = mcp.get_lifespan_context()
    sheets_service = context.sheets_service
    
    # Get spreadsheet metadata including named ranges
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    info = {
        "title": spreadsheet.get('properties', {}).get('title', 'Unknown'),
        "sheets": [
            {
                "title": sheet['properties']['title'],
                "sheetId": sheet['properties']['sheetId'],
                "gridProperties": sheet['properties'].get('gridProperties', {})
            }
            for sheet in spreadsheet.get('sheets', [])
        ],
        "named_ranges": [
            {
                "name": nr['name'],
                "namedRangeId": nr['namedRangeId'],
                "range": nr.get('range', {})
            }
            for nr in spreadsheet.get('namedRanges', [])
        ]
    }

    return json.dumps(info, indent=2)


@tool(
    annotations=ToolAnnotations(
        title="List Spreadsheets",
        readOnlyHint=True,
    ),
)
def list_spreadsheets(folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all spreadsheets in the specified Google Drive folder.
    If no folder is specified, uses the configured default folder or lists from 'My Drive'.
    
    Args:
        folder_id: Optional Google Drive folder ID to search in.
                  If not provided, uses the configured default folder or searches 'My Drive'.
    
    Returns:
        List of spreadsheets with their ID and title
    """
    drive_service = ctx.request_context.lifespan_context.drive_service
    # Use provided folder_id or fall back to configured default
    target_folder_id = folder_id or ctx.request_context.lifespan_context.folder_id
    
    query = "mimeType='application/vnd.google-apps.spreadsheet'"
    
    # If a specific folder is provided or configured, search only in that folder
    if target_folder_id:
        query += f" and '{target_folder_id}' in parents"
        print(f"Searching for spreadsheets in folder: {target_folder_id}")
    else:
        print("Searching for spreadsheets in 'My Drive'")
    
    # List spreadsheets
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        corpora='allDrives',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name)',
        orderBy='modifiedTime desc'
    ).execute()
    
    spreadsheets = results.get('files', [])
    
    return [{'id': sheet['id'], 'title': sheet['name']} for sheet in spreadsheets]


@tool(
    annotations=ToolAnnotations(
        title="List Folders",
        readOnlyHint=True,
    ),
)
def list_folders(parent_folder_id: Optional[str] = None, include_shared: bool = True, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all folders in the specified Google Drive folder.
    If no parent folder is specified, lists folders from 'My Drive' root and optionally shared folders.

    Args:
        parent_folder_id: Optional Google Drive folder ID to search within.
                         If not provided, searches the root of 'My Drive'.
        include_shared: If True (default), also includes folders shared with the user when
                       no parent_folder_id is specified.

    Returns:
        List of folders with their ID, name, and parent information
    """
    drive_service = ctx.request_context.lifespan_context.drive_service

    base_query = "mimeType='application/vnd.google-apps.folder'"

    if parent_folder_id:
        print(f"Searching for folders in parent folder: {parent_folder_id}")
        results = drive_service.files().list(
            q=f"{base_query} and '{parent_folder_id}' in parents",
            spaces='drive',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields='files(id, name, parents)',
            orderBy='name'
        ).execute()
        folders = results.get('files', [])
    else:
        # Fetch My Drive root folders
        print("Searching for folders in 'My Drive' root")
        my_drive_results = drive_service.files().list(
            q=f"{base_query} and 'root' in parents",
            spaces='drive',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields='files(id, name, parents)',
            orderBy='name'
        ).execute()
        folders = my_drive_results.get('files', [])

        if include_shared:
            print("Also searching for folders shared with me")
            shared_results = drive_service.files().list(
                q=f"{base_query} and sharedWithMe=true",
                spaces='drive',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields='files(id, name, parents)',
                orderBy='name'
            ).execute()
            # Deduplicate by ID
            seen = {f['id'] for f in folders}
            for f in shared_results.get('files', []):
                if f['id'] not in seen:
                    folders.append(f)
                    seen.add(f['id'])

    return [
        {
            'id': folder['id'],
            'name': folder['name'],
            'parent': folder.get('parents', ['root'])[0] if folder.get('parents') else 'root'
        }
        for folder in folders
    ]




@tool(
    annotations=ToolAnnotations(
        title="Search Spreadsheets by Name or Content",
        readOnlyHint=True,
    ),
)
def search_spreadsheets(query: str,
                        max_results: int = 20,
                        ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Search for spreadsheets in Google Drive by name or content.

    Args:
        query: Search query string. Searches in file name and content.
               Examples: "budget 2024", "sales report", "project tracker"
        max_results: Maximum number of results to return (default 20, max 100)

    Returns:
        List of matching spreadsheets with their ID, name, and metadata
    """
    drive_service = ctx.request_context.lifespan_context.drive_service

    # Limit max_results to reasonable bounds
    max_results = min(max(1, max_results), 100)

    # Build the search query for Google Drive
    # Search only for spreadsheets and match the query in name or fullText
    search_query = (
        f"mimeType='application/vnd.google-apps.spreadsheet' and "
        f"(name contains '{query}' or fullText contains '{query}')"
    )

    try:
        results = drive_service.files().list(
            q=search_query,
            pageSize=max_results,
            spaces='drive',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields='files(id, name, createdTime, modifiedTime, owners, webViewLink)',
            orderBy='modifiedTime desc'
        ).execute()

        files = results.get('files', [])

        return [
            {
                'id': f['id'],
                'name': f['name'],
                'created_time': f.get('createdTime'),
                'modified_time': f.get('modifiedTime'),
                'owners': [owner.get('emailAddress') for owner in f.get('owners', [])],
                'web_link': f.get('webViewLink')
            }
            for f in files
        ]
    except Exception as e:
        return [{'error': f'Search failed: {str(e)}'}]


def _column_index_to_letter(index: int) -> str:
    """Convert 0-based column index to A1 notation letter (0='A', 25='Z', 26='AA', etc.)"""
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result



@tool(
    annotations=ToolAnnotations(
        title="Find Cells",
        readOnlyHint=True,
    ),
)
def find_in_spreadsheet(spreadsheet_id: str,
                        query: str,
                        sheet: Optional[str] = None,
                        case_sensitive: bool = False,
                        max_results: int = 50,
                        ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Find cells containing a specific value in a Google Spreadsheet.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        query: The text to search for in cell values
        sheet: Optional sheet name to search in. If not provided, searches all sheets.
        case_sensitive: Whether the search should be case-sensitive (default False)
        max_results: Maximum number of results to return (default 50)

    Returns:
        List of found cells with their location (sheet, cell in A1 notation) and value
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = []

    try:
        # Get spreadsheet metadata to find all sheets
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='sheets(properties(title,sheetId))'
        ).execute()

        sheets_to_search = []
        for s in spreadsheet.get('sheets', []):
            sheet_title = s.get('properties', {}).get('title')
            if sheet is None or sheet_title == sheet:
                sheets_to_search.append(sheet_title)

        if not sheets_to_search:
            return [{'error': f"Sheet '{sheet}' not found"}]

        search_query = query if case_sensitive else query.lower()

        for sheet_name in sheets_to_search:
            if len(results) >= max_results:
                break

            # Get all data from the sheet
            response = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()

            values = response.get('values', [])

            for row_idx, row in enumerate(values):
                if len(results) >= max_results:
                    break

                for col_idx, cell_value in enumerate(row):
                    if len(results) >= max_results:
                        break

                    cell_str = str(cell_value)
                    compare_value = cell_str if case_sensitive else cell_str.lower()

                    if search_query in compare_value:
                        cell_ref = f"{_column_index_to_letter(col_idx)}{row_idx + 1}"
                        results.append({
                            'sheet': sheet_name,
                            'cell': cell_ref,
                            'value': cell_value
                        })

        return results

    except Exception as e:
        return [{'error': f'Search failed: {str(e)}'}]


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    from starlette.responses import JSONResponse
    return JSONResponse({"status": "healthy"})


def main():
    # Log tool filtering configuration if enabled
    if ENABLED_TOOLS is not None:
        print(f"Tool filtering enabled. Active tools: {', '.join(sorted(ENABLED_TOOLS))}")
    else:
        print("Tool filtering disabled. All tools are enabled.")
    
    # Run the server
    transport = "stdio"
    for i, arg in enumerate(sys.argv):
        if arg == "--transport" and i + 1 < len(sys.argv):
            transport = sys.argv[i + 1]
            break

    mcp.run(transport=transport)
