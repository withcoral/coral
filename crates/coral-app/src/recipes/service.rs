//! Implements the gRPC `RecipeService`.

use coral_api::v1::recipe_service_server::RecipeService as RecipeServiceApi;
use coral_api::v1::{
    AddRecipeRequest, AddRecipeResponse, ListRecipesRequest, ListRecipesResponse, Recipe,
    RecipeArgument, RecipeOrigin as ProtoRecipeOrigin, RecipePublishedSurface, RecipeResultColumn,
    RecipeTableFunctionPublish, RemoveRecipeRequest, RemoveRecipeResponse,
    recipe_published_surface,
};
use coral_engine::{RecipeRuntimeArgumentType, RecipeRuntimeDefinition, RecipeRuntimePublish};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::query::manager::QueryManager;
use crate::recipes::manager::{RecipeListing, RecipeManager};
use crate::recipes::model::{RecipeName, RecipeOrigin};
use crate::transport::{grpc_span, instrument_grpc, query_status, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct RecipeService {
    recipes: RecipeManager,
    queries: QueryManager,
}

impl RecipeService {
    pub(crate) fn new(recipe_manager: RecipeManager, query_manager: QueryManager) -> Self {
        Self {
            recipes: recipe_manager,
            queries: query_manager,
        }
    }
}

#[tonic::async_trait]
impl RecipeServiceApi for RecipeService {
    async fn add_recipe(
        &self,
        request: Request<AddRecipeRequest>,
    ) -> Result<Response<AddRecipeResponse>, Status> {
        let span = grpc_span(&request);
        let recipes = self.recipes.clone();
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let runtime_recipe = queries
                .validate_recipe_yaml(&workspace_name, &inner.yaml)
                .await
                .map_err(query_status)?;
            let recipe = recipes
                .install_validated_user_recipe(&workspace_name, &inner.yaml, &runtime_recipe)
                .map_err(app_status)?;
            Ok(Response::new(AddRecipeResponse {
                name: recipe.name.as_str().to_string(),
                origin: proto_recipe_origin(recipe.origin) as i32,
                enabled: recipe.enabled,
            }))
        })
        .await
    }

    async fn list_recipes(
        &self,
        request: Request<ListRecipesRequest>,
    ) -> Result<Response<ListRecipesResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let recipes = queries
                .list_recipes(&workspace_name)
                .map_err(query_status)?
                .into_iter()
                .map(recipe_listing_to_proto)
                .collect();
            Ok(Response::new(ListRecipesResponse { recipes }))
        })
        .await
    }

    async fn remove_recipe(
        &self,
        request: Request<RemoveRecipeRequest>,
    ) -> Result<Response<RemoveRecipeResponse>, Status> {
        let span = grpc_span(&request);
        let recipes = self.recipes.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let recipe_name = RecipeName::parse(&inner.name).map_err(app_status)?;
            recipes
                .remove_user_recipe(&workspace_name, &recipe_name)
                .map_err(app_status)?;
            Ok(Response::new(RemoveRecipeResponse {}))
        })
        .await
    }
}

fn recipe_listing_to_proto(listing: RecipeListing) -> Recipe {
    runtime_recipe_to_proto(
        listing.definition,
        proto_recipe_origin(listing.origin),
        listing.enabled,
    )
}

fn runtime_recipe_to_proto(
    recipe: RecipeRuntimeDefinition,
    origin: ProtoRecipeOrigin,
    enabled: bool,
) -> Recipe {
    Recipe {
        name: recipe.name,
        description: recipe.description,
        arguments: recipe
            .arguments
            .into_iter()
            .map(|argument| RecipeArgument {
                name: argument.name,
                data_type: recipe_argument_type(argument.data_type).to_string(),
                required: argument.required,
                description: argument.description,
            })
            .collect(),
        publish: vec![recipe_publish_to_proto(recipe.publish)],
        result_columns: recipe
            .result_columns
            .into_iter()
            .map(|column| RecipeResultColumn {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
                description: column.description,
            })
            .collect(),
        origin: origin as i32,
        enabled,
    }
}

fn recipe_publish_to_proto(publish: RecipeRuntimePublish) -> RecipePublishedSurface {
    let target = recipe_published_surface::Target::TableFunction(RecipeTableFunctionPublish {
        schema: publish.table_function.schema,
        name: publish.table_function.name,
        description: publish.table_function.description,
    });
    RecipePublishedSurface {
        target: Some(target),
    }
}

fn proto_recipe_origin(origin: RecipeOrigin) -> ProtoRecipeOrigin {
    match origin {
        RecipeOrigin::Bundled => ProtoRecipeOrigin::Bundled,
        RecipeOrigin::User => ProtoRecipeOrigin::User,
    }
}

fn recipe_argument_type(data_type: RecipeRuntimeArgumentType) -> &'static str {
    match data_type {
        RecipeRuntimeArgumentType::String => "string",
        RecipeRuntimeArgumentType::Integer => "integer",
        RecipeRuntimeArgumentType::Boolean => "boolean",
    }
}
