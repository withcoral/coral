use serde::{Deserialize, Serialize};

use crate::v4::ir::{HttpMethod, IrInputLocation, IrScalarType};
use crate::{PaginationSpec, ResponseSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestExecutionAttachment {
    pub method: HttpMethod,
    pub path_template: String,
    pub parameters: Vec<RestParameterBinding>,
    pub request_body: Option<RestRequestBody>,
    pub response: RestResponseAttachment,
    pub pagination: PaginationSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestRequestBody {
    pub required: bool,
    pub media_type: String,
    pub type_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestParameterBinding {
    pub input_name: String,
    pub location: IrInputLocation,
    pub wire_name: String,
    pub required: bool,
    pub data_type: IrScalarType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestResponseAttachment {
    pub status_code: u16,
    pub media_type: String,
    pub response: ResponseSpec,
}
