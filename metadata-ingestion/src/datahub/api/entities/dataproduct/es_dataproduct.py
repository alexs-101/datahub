from __future__ import annotations
import json
import re
from typing import Optional, Union, List, Dict
from pathlib import Path
from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.cli.specific.file_loader import load_file
from pydantic import BaseModel, Field
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
    make_term_urn,
    make_user_urn,
    make_domain_urn,
    make_owner_urn,
    validate_ownership_type,
    OwnerType
)
from urllib.parse import urlparse
from typing import Iterable, Union
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataProductAssociationClass as DataProductAssociation,
    DataProductPropertiesClass as DataProductProperties,
    GlobalTagsClass as GlobalTags,
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass as GlossaryTerms,
    MetadataChangeProposalClass,
    KafkaAuditHeaderClass,
    OwnerClass as Owner,
    OwnershipTypeClass,
    SystemMetadataClass,
    TagAssociationClass as Tag,
    ChangeTypeClass,
    GenericAspectClass
)


class _EsOutputPortsDetails(BaseModel):
    class Config:
        allow_population_by_field_name = True

    catalog: str
    schema_: str = Field(alias='schema')
    table: str
    env: str


class _EsOutputPorts(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: str
    description: str
    details: _EsOutputPortsDetails


class _EsSchemaProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: str = Field(default = "", alias='type')
    description: str = Field(default = "", alias='description')


class _EsSchemaXProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    property_uri: str = Field(default = "", alias='propertyUri')
    relation_type: str = Field(default = "", alias='relationType')
    required: bool = Field(default = False, alias='required')
    is_primary_id: bool = Field(default = False, alias='isPrimaryId')
    compliance_requirements: str = Field(default_factory = list, alias='complianceRequirements')
    transformations: str = Field(default_factory = list, alias='transformations')


class _EsSchema(BaseModel):
    class Config:
        allow_population_by_field_name = True

    properties: Dict[str, _EsSchemaProperty] = Field(default_factory=dict, alias='properties')
    x_context: Dict[str, _EsSchemaXProperty] = Field(default_factory=dict, alias='x-context')


class _EsDataProduct(BaseModel):
    class Config:
        allow_population_by_field_name = True

    data_product_id: str = Field(alias='dataProductId')
    name: str = Field(alias='name')
    description: str = Field(alias='description')
    version: str = Field(alias='version')
    domain: str = Field(alias='domain')
    data_alignment_type: str = Field(alias='dataAlignmentType')
    source_systems: List[str] = Field(alias='sourceSystems')
    processing: str = Field(alias='processing')
    framework: str = Field(alias='framework')
    data_contract: str = Field(alias='dataContract')
    slas: str = Field(alias='SLAs')
    use_cases: List[str] = Field(alias='useCases')
    subject_matter_experts: List[str] = Field(alias='subjectMatterExperts')
    country_of_origin: List[str] = Field(alias='countryOfOrigin')
    data_classification: str = Field(alias='dataClassification')
    compliance_requirements: List[str] = Field(alias='complianceRequirements')
    acceptable_use: str = Field(alias='acceptableUse')
    created_by: str = Field(alias='createdBy')
    last_modified: str = Field(alias='lastModified')
    update_frequency: str = Field(alias='updateFrequency')
    output_ports: List[_EsOutputPorts] = Field(alias='outputPorts')
    schema_: _EsSchema = Field(default = _EsSchema, alias='schema')

    
    def get_data_product_external_url(self):
        try:
            urlparse(self.data_product_id)
            return self.data_product_id
        except ValueError:
            return ""
    
    
    def get_data_product_id(self):
        try:
            tmp = urlparse(self.data_product_id).path.strip("/")
        except ValueError:
            tmp = self.data_product_id

        return re.sub(r"\W+", "_", tmp)
    
    
    def get_data_product_assets(self):
        return [
            make_dataset_urn("databricks", f"{op.details.catalog}.{op.details.schema_}.{op.details.table}", op.details.env)
            for op in self.output_ports
        ]


    def get_data_product_display_name(self):
        return self.name


    def get_data_product_description(self):
        return self.description
    

    def get_data_product_domain(self):
        return make_domain_urn(self.domain)
    

    def get_data_product_owners(self):
        return [
            make_owner_urn(user, OwnerType.USER) 
            for user in self.subject_matter_experts
        ]


    def get_data_product_schema_properties(self):
        return [
            EsDataProductSchemaProperty(
                name=k,
                description=v.description,
                type=v.type,
                property_uri=self.schema_.x_context[k].property_uri,
                relation_type=self.schema_.x_context[k].relation_type,
                required=self.schema_.x_context[k].required,
                is_primary_id=self.schema_.x_context[k].is_primary_id,
                compliance_requirements=self.array_to_str(self.schema_.x_context[k].compliance_requirements),
                transformations=self.array_to_str(self.schema_.x_context[k].transformations)
            )
            for k, v in self.schema_.properties.items()
        ]
    

    @classmethod
    def array_to_str(cls, a: List[str]):
        return ', '.join(a)


class EsDataProductSchemaProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str = Field(default = "", alias='name')
    description: str = Field(default = "", alias='description')
    type: str = Field(default = "", alias='type')
    property_uri: str = Field(default="", alias='propertyUri')
    relation_type: str = Field(default="", alias='relationType')
    required: bool = Field(default=False, alias='required')
    is_primary_id: bool = Field(default=False, alias='isPrimaryId')
    compliance_requirements: str = Field(default_factory = list,alias='complianceRequirements')
    transformations: str = Field(default_factory = list, alias='transformations')


class EsDataProduct(DataProduct):
    schema_: Optional[List[EsDataProductSchemaProperty]] = None

    @classmethod
    def from_es_json(cls, file: Path) -> EsDataProduct:
        
        es_data_product = _EsDataProduct.parse_obj(load_file(file))

        return EsDataProduct(
            id = es_data_product.get_data_product_id(),
            
            domain = es_data_product.get_data_product_domain(), # emmit if not emmitted?
            
            assets = es_data_product.get_data_product_assets(), # emmit if not emmitted?
            display_name = es_data_product.get_data_product_display_name(),
            
            owners = es_data_product.get_data_product_owners(), # emmit if not emmitted?

            description = es_data_product.get_data_product_description(),
            tags = [],
            terms = [],
            properties = {
                "dataProductId": es_data_product.data_product_id,
                "name": es_data_product.name,
                "description": es_data_product.description,
                "version": es_data_product.version,
                "domain": es_data_product.domain,
                "dataAlignmentType": es_data_product.data_alignment_type,
                "sourceSystems": _EsDataProduct.array_to_str(es_data_product.source_systems),
                "processing": es_data_product.processing,
                "framework": es_data_product.framework,
                "dataContract": es_data_product.data_contract,
                "SLAs": es_data_product.slas,
                "useCases": _EsDataProduct.array_to_str(es_data_product.use_cases),
                "subjectMatterExperts": _EsDataProduct.array_to_str(es_data_product.subject_matter_experts),
                "countryOfOrigin": _EsDataProduct.array_to_str(es_data_product.country_of_origin),
                "dataClassification": es_data_product.data_classification,
                "complianceRequirements" : _EsDataProduct.array_to_str(es_data_product.compliance_requirements),
                "acceptableUse": es_data_product.acceptable_use,
                "createdBy": es_data_product.created_by,
                "lastModified": es_data_product.last_modified,
                "updateFrequency": es_data_product.update_frequency
            },
            external_url = es_data_product.get_data_product_external_url(),
            schema_=es_data_product.get_data_product_schema_properties()
        )
    
    def generate_mcp(
        self, upsert: bool
    ) -> Iterable[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        yield from super().generate_mcp(upsert)
        yield from self.generate_es_mcp(upsert)

    def generate_es_mcp(
        self, upsert: bool
    ) -> Iterable[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        es_data_product_schema_properties = {
            "esDataProductSchemaProperties": [sp.dict(by_alias = True) for sp in self.schema_]
        }
        yield MetadataChangeProposalClass(
            entityType="dataProduct",
            entityUrn=self.urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="esDataProductSchema",
            aspect=GenericAspectClass(
                contentType="application/json",
                value=json.dumps(es_data_product_schema_properties).encode("utf-8"),
            ),
        )
