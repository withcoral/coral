//! RMCP server implementation for Coral's stdio MCP surface.

use coral_client::AppClient;
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerInfo, Tool,
    },
    service::{RequestContext, RoleServer},
};

#[cfg(feature = "code-mode")]
use crate::code_mode::{CodeModeState, schema_tables, wait_description};
#[cfg(feature = "code-mode")]
use crate::surface::{exec_arguments, exec_tool, wait_arguments, wait_tool};
use crate::{
    McpOptions,
    bridge::{BridgeCallOutcome, BridgeOptions, CoralToolBridge, finish_bridge_call},
    surface::{
        describe_table_tool, feedback_tool, guide_resource, guide_resource_content,
        initial_instructions, list_catalog_tool, list_columns_tool, search_catalog_tool, sql_tool,
        status_to_error_data, tables_resource, tables_resource_content,
    },
    telemetry,
};

pub(crate) struct CoralMcpServer {
    bridge: CoralToolBridge,
    #[cfg(feature = "code-mode")]
    code_mode: Option<CodeModeState>,
    options: McpOptions,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        let bridge_options = BridgeOptions {
            feedback_enabled: options.feedback_enabled,
        };
        let bridge = CoralToolBridge::new(app, bridge_options);
        Self {
            #[cfg(feature = "code-mode")]
            code_mode: options
                .code_mode_surface_enabled()
                .then(|| CodeModeState::new(bridge.clone())),
            bridge,
            options,
        }
    }

    async fn dispatch_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<BridgeCallOutcome, ErrorData> {
        #[cfg(feature = "code-mode")]
        if let Some(code_mode) = &self.code_mode {
            match request.name.as_ref() {
                "exec" => {
                    let nested_tools = self.finite_tools().await?;
                    let arguments = exec_arguments(request.arguments.as_ref())?;
                    return Ok(code_mode.execute(arguments, &nested_tools).await);
                }
                "wait" => {
                    let arguments = wait_arguments(request.arguments.as_ref())?;
                    return Ok(code_mode.wait(arguments).await);
                }
                _ if self.options.code_mode_only_enabled => {
                    return Err(ErrorData::invalid_params(
                        format!("tool '{}' not found", request.name),
                        None,
                    ));
                }
                _ => {}
            }
        }

        self.bridge
            .call(request.name.as_ref(), request.arguments.as_ref())
            .await
    }

    async fn finite_tools(&self) -> Result<Vec<Tool>, ErrorData> {
        let (sources, visible_table_count, visible_function_count) = self
            .bridge
            .load_sources_and_catalog_counts()
            .await
            .map_err(|status| status_to_error_data(&status))?;
        let mut tools = vec![
            sql_tool(&sources, visible_table_count),
            list_catalog_tool(visible_table_count, visible_function_count),
            search_catalog_tool(visible_table_count, visible_function_count),
            describe_table_tool(),
            list_columns_tool(),
        ];
        if self.options.feedback_enabled {
            tools.push(feedback_tool());
        }
        Ok(tools)
    }

    async fn visible_tools(&self) -> Result<Vec<Tool>, ErrorData> {
        #[cfg(feature = "code-mode")]
        {
            let mut tools = self.finite_tools().await?;
            if self.code_mode.is_some() {
                let schema_tables = self.code_mode_schema_tables().await;
                let code_mode_open_world = tools.iter().any(|tool| {
                    tool.annotations
                        .as_ref()
                        .and_then(|annotations| annotations.open_world_hint)
                        .unwrap_or(false)
                });
                let exec = exec_tool(
                    CodeModeState::exec_description(&tools, &schema_tables),
                    code_mode_open_world,
                );
                let wait = wait_tool(wait_description(), code_mode_open_world);
                if self.options.code_mode_only_enabled {
                    return Ok(vec![exec, wait]);
                }
                tools.push(exec);
                tools.push(wait);
                Ok(tools)
            } else {
                Ok(tools)
            }
        }
        #[cfg(not(feature = "code-mode"))]
        {
            self.finite_tools().await
        }
    }

    #[cfg(feature = "code-mode")]
    async fn code_mode_schema_tables(&self) -> Vec<coral_code_mode::CodeModeSchemaTable> {
        const MAX_DECLARED_TABLES: usize = 16;

        let Ok(summaries) = self
            .bridge
            .load_limited_table_summaries(
                u32::try_from(MAX_DECLARED_TABLES).expect("declared table limit fits u32"),
            )
            .await
        else {
            return Vec::new();
        };
        let mut tables = Vec::new();
        for summary in summaries {
            if let Ok(response) = self
                .bridge
                .load_table_description(&summary.schema_name, &summary.name)
                .await
                && let Some(table) = response.table
            {
                tables.push(table);
            }
        }
        schema_tables(&tables)
    }
}

impl ServerHandler for CoralMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::new("coral", env!("CARGO_PKG_VERSION")))
        .with_instructions(initial_instructions())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let span = telemetry::list_tools_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            Ok(ListToolsResult::with_all_items(self.visible_tools().await?))
        })
        .await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let span =
            telemetry::call_tool_span(request.name.as_ref(), self.options.trace_parent.as_deref());
        telemetry::record_tool_request_details(
            &span,
            request.name.as_ref(),
            request.arguments.as_ref(),
            self.code_mode_runtime_enabled(),
        );
        let outcome = telemetry::instrument(span.clone(), self.dispatch_tool(request)).await;
        finish_bridge_call(&span, outcome)
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let span = telemetry::list_resources_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            let (sources, visible_table_count, visible_function_count) = self
                .bridge
                .load_sources_and_catalog_counts()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            Ok(ListResourcesResult::with_all_items(vec![
                guide_resource(&sources, visible_table_count, visible_function_count),
                tables_resource(visible_table_count),
            ]))
        })
        .await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        let span = telemetry::read_resource_span(
            request.uri.as_str(),
            self.options.trace_parent.as_deref(),
        );
        telemetry::instrument_protocol(span, async {
            match request.uri.as_str() {
                "coral://guide" => {
                    let (sources, tables, table_function_schema_names) = self
                        .bridge
                        .load_sources_and_guide_catalog()
                        .await
                        .map_err(|status| status_to_error_data(&status))?;
                    Ok(ReadResourceResult::new(vec![
                        ResourceContents::text(
                            guide_resource_content(&sources, &tables, &table_function_schema_names),
                            request.uri,
                        )
                        .with_mime_type("text/markdown"),
                    ]))
                }
                "coral://tables" => {
                    let tables = self
                        .bridge
                        .load_all_table_summaries()
                        .await
                        .map_err(|status| status_to_error_data(&status))?;
                    let text = tables_resource_content(&tables)
                        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
                    Ok(ReadResourceResult::new(vec![
                        ResourceContents::text(text, request.uri)
                            .with_mime_type("application/json"),
                    ]))
                }
                _ => Err(ErrorData::resource_not_found(
                    format!("resource '{}' not found", request.uri),
                    None,
                )),
            }
        })
        .await
    }
}

impl CoralMcpServer {
    #[cfg(feature = "code-mode")]
    fn code_mode_runtime_enabled(&self) -> bool {
        self.code_mode.is_some()
    }

    #[cfg(not(feature = "code-mode"))]
    fn code_mode_runtime_enabled(&self) -> bool {
        false
    }
}

#[cfg(feature = "code-mode")]
impl McpOptions {
    fn code_mode_surface_enabled(&self) -> bool {
        self.code_mode_enabled || self.code_mode_only_enabled
    }
}
