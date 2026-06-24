//! Saved function SQL parameter binding helpers.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::runtime::query::QueryRuntimeAdapter;
use crate::{
    CoreError, QueryParameters, SavedFunctionRuntimeArgument, SavedFunctionRuntimeDefinition,
    SavedFunctionRuntimeImplementation,
};

fn saved_function_sql(saved_function: &SavedFunctionRuntimeDefinition) -> &str {
    let SavedFunctionRuntimeImplementation::CoralSql { query } = &saved_function.implementation;
    query
}

pub(crate) async fn validate_saved_function(
    query_runtime: &QueryRuntimeAdapter,
    saved_function: &SavedFunctionRuntimeDefinition,
    arguments: &QueryParameters,
) -> Result<Arc<Schema>, CoreError> {
    let params =
        SavedFunctionArgumentBinding::new(saved_function, arguments).into_query_params()?;
    query_runtime
        .infer_sql_schema(saved_function_sql(saved_function), &params)
        .await
}

struct SavedFunctionArgumentBinding<'a> {
    saved_function: &'a SavedFunctionRuntimeDefinition,
    arguments: &'a QueryParameters,
}

impl<'a> SavedFunctionArgumentBinding<'a> {
    fn new(
        saved_function: &'a SavedFunctionRuntimeDefinition,
        arguments: &'a QueryParameters,
    ) -> Self {
        Self {
            saved_function,
            arguments,
        }
    }

    fn into_query_params(self) -> Result<QueryParameters, CoreError> {
        self.reject_duplicate_argument_definitions()?;
        self.reject_unknown_arguments()?;

        let mut params = self.arguments.clone();
        for argument in &self.saved_function.arguments {
            self.bind_argument(argument, &mut params)?;
        }
        Ok(params)
    }

    fn reject_duplicate_argument_definitions(&self) -> Result<(), CoreError> {
        let mut seen = BTreeSet::new();
        for argument in &self.saved_function.arguments {
            if !seen.insert(argument.name.as_str()) {
                return Err(CoreError::InvalidInput(format!(
                    "saved_function '{}' argument '{}' is declared more than once",
                    self.saved_function.name, argument.name
                )));
            }
        }
        Ok(())
    }

    fn reject_unknown_arguments(&self) -> Result<(), CoreError> {
        if let Some(argument_name) = self.arguments.keys().find(|argument_name| {
            self.saved_function
                .arguments
                .iter()
                .all(|argument| argument.name != **argument_name)
        }) {
            return Err(CoreError::InvalidInput(format!(
                "saved_function '{}' received unknown argument '{}'",
                self.saved_function.name, argument_name
            )));
        }
        Ok(())
    }

    fn bind_argument(
        &self,
        argument: &SavedFunctionRuntimeArgument,
        params: &mut QueryParameters,
    ) -> Result<(), CoreError> {
        match params.get(&argument.name) {
            Some(value) if !argument.data_type.accepts(value) => {
                Err(CoreError::InvalidInput(format!(
                    "saved_function '{}' argument '{}' expected {}, got {}",
                    self.saved_function.name,
                    argument.name,
                    argument.data_type.as_str(),
                    value.type_name()
                )))
            }
            Some(value) if argument.required && value.is_null() => {
                Err(CoreError::InvalidInput(format!(
                    "saved_function '{}' argument '{}' is required and cannot be null",
                    self.saved_function.name, argument.name
                )))
            }
            Some(_) => Ok(()),
            None if argument.required => Err(CoreError::InvalidInput(format!(
                "saved_function '{}' is missing required argument '{}'",
                self.saved_function.name, argument.name
            ))),
            None => {
                params.insert(argument.name.clone(), argument.data_type.typed_null_value());
                Ok(())
            }
        }
    }
}
