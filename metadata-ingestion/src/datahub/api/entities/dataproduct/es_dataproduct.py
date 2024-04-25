from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from datahub.cli.specific.file_loader import load_file
from datahub.emitter.mce_builder import (
    OwnerType,
    make_data_platform_urn,
    make_dataset_urn,
    make_domain_urn,
    make_owner_urn,
    make_term_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NullTypeClass,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsClass,
    ChangeTypeClass,
    DataProductAssociationClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    GenericAspectClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    GlossaryTermSnapshotClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
)


class _EsOutputPortsDetails(BaseModel):
    class Config:
        allow_population_by_field_name = True

    catalog: str
    schema_: str = Field(alias="schema")
    table: str
    env: str


class _EsOutputPorts(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: str
    description: str
    details: _EsOutputPortsDetails

    def get_fully_qualified_name(self):
        return f"{self.details.catalog}.{self.details.schema_}.{self.details.table}"

    def get_urn(self):
        return make_dataset_urn(
            "databricks", self.get_fully_qualified_name(), self.details.env
        )


class _EsSchemaProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: str = Field(default="", alias="type")
    description: str = Field(default="", alias="description")


class EsSchemaProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    name: str = Field(default="", alias="name")
    description: str = Field(default="", alias="description")
    type: str = Field(default="", alias="type")
    property_uri: str = Field(default="", alias="propertyUri")
    relation_type: str = Field(default="", alias="relationType")
    required: bool = Field(default=False, alias="required")
    is_primary_id: bool = Field(default=False, alias="isPrimaryId")
    compliance_requirements: str = Field(default="", alias="complianceRequirements")
    transformations: str = Field(default="", alias="transformations")

    def get_term(self):
        try:
            tmp = urlparse(self.property_uri).path.strip("/")
        except ValueError:
            tmp = self.property_uri

        return tmp

    def get_term_name(self):
        return self.get_term().split("/")[-1]

    def get_term_browse_path(self):
        return self.get_term().split("/")[:-1]

    def get_term_urn(self):
        return make_term_urn(self.get_term().replace("/", "_"))


class _EsSchemaXProperty(BaseModel):
    class Config:
        allow_population_by_field_name = True

    property_uri: str = Field(default="", alias="propertyUri")
    relation_type: str = Field(default="", alias="relationType")
    required: bool = Field(default=False, alias="required")
    is_primary_id: bool = Field(default=False, alias="isPrimaryId")
    compliance_requirements: List[str] = Field(
        default_factory=list, alias="complianceRequirements"
    )
    transformations: List[str] = Field(default_factory=list, alias="transformations")


class _EsSchema(BaseModel):
    class Config:
        allow_population_by_field_name = True

    properties: Dict[str, _EsSchemaProperty] = Field(
        default_factory=dict, alias="properties"
    )
    x_context: Dict[str, _EsSchemaXProperty] = Field(
        default_factory=dict, alias="x-context"
    )


class _EsDataProduct(BaseModel):
    class Config:
        allow_population_by_field_name = True

    data_product_id: str = Field(alias="dataProductId")
    name: str = Field(alias="name")
    description: str = Field(alias="description")
    version: str = Field(alias="version")
    domain: str = Field(alias="domain")
    data_alignment_type: str = Field(alias="dataAlignmentType")
    source_systems: List[str] = Field(alias="sourceSystems")
    processing: str = Field(alias="processing")
    framework: str = Field(alias="framework")
    data_contract: str = Field(alias="dataContract")
    slas: str = Field(alias="SLAs")
    use_cases: List[str] = Field(alias="useCases")
    subject_matter_experts: List[str] = Field(alias="subjectMatterExperts")
    country_of_origin: List[str] = Field(alias="countryOfOrigin")
    data_classification: str = Field(alias="dataClassification")
    compliance_requirements: List[str] = Field(alias="complianceRequirements")
    acceptable_use: str = Field(alias="acceptableUse")
    created_by: str = Field(alias="createdBy")
    last_modified: str = Field(alias="lastModified")
    update_frequency: str = Field(alias="updateFrequency")
    output_ports: List[_EsOutputPorts] = Field(alias="outputPorts")
    schema_: Optional[_EsSchema] = Field(default=None, alias="schema")

    def get_data_product_urn(self):
        try:
            tmp = urlparse(self.data_product_id).path.strip("/")
        except ValueError:
            tmp = self.data_product_id

        tmp = re.sub(r"\W+", "_", tmp)

        return f"urn:li:dataProduct:{tmp}"

    def get_data_product_dataset_urns(self):
        return [op.get_urn() for op in self.output_ports]

    def get_data_product_display_name(self):
        return self.name

    def get_data_product_description(self):
        return self.description

    def get_data_product_domain_urn(self):
        return make_domain_urn(self.get_data_product_domain())

    def get_data_product_domain(self):
        return self.domain

    def get_data_product_owners(self):
        return [
            make_owner_urn(user, OwnerType.USER) for user in self.subject_matter_experts
        ]

    def get_data_product_schema_properties(self) -> Iterable[EsSchemaProperty]:
        def __to_dict(v: BaseModel) -> dict:
            return {k1: str(v1) for k1, v1 in v.dict(by_alias=True).items()}

        if not self.schema_:
            return []

        return [
            EsSchemaProperty(
                name=name, **__to_dict(v), **__to_dict(self.schema_.x_context[name])
            )
            for name, v in self.schema_.properties.items()
        ]

    def get_data_product_schema_properties_patched(self) -> Iterable[EsSchemaProperty]:
        for p in self.get_data_product_schema_properties():
            yield p.copy(
                update={
                    "propertyUri": f"http://localhost:9002/glossaryTerm/{p.get_term_urn()}"
                }
            )

    def get_data_product_custom_properties(self) -> Dict[str, str]:
        return {
            k: str(v)
            for k, v in self.dict(by_alias=True).items()
            if k not in ["schema", "outputPorts", "dataProductId"]
        }

    def _mint_auditstamp(self, message: str) -> AuditStampClass:
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    @classmethod
    def load_from_json(cls, file: Path) -> _EsDataProduct:
        return _EsDataProduct.parse_obj(load_file(file))

    def generate_mcp(
        self, graph: DataHubGraph
    ) -> Iterable[
        Union[
            MetadataChangeProposalWrapper,
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
        ]
    ]:

        yield from self.generate_domain_mcp_if_not_exists(graph)
        yield from self.generate_datasets_mcps_if_not_exists(graph)

        yield MetadataChangeProposalWrapper(
            entityUrn=self.get_data_product_urn(),
            aspect=DataProductPropertiesClass(
                name=self.get_data_product_display_name(),
                description=self.get_data_product_description(),
                assets=[
                    DataProductAssociationClass(
                        destinationUrn=asset,
                    )
                    for asset in self.get_data_product_dataset_urns()
                ],
                customProperties=self.get_data_product_custom_properties(),
                externalUrl=None,
            ),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self.get_data_product_urn(),
            aspect=DomainsClass(
                domains=[self.get_data_product_domain_urn()]  # TODO: resolve it first
            ),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self.get_data_product_urn(),
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=make_user_urn(o), type=OwnershipTypeClass.BUSINESS_OWNER
                    )
                    for o in self.get_data_product_owners()
                ]
            ),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self.get_data_product_urn(), aspect=StatusClass(removed=False)
        )

        yield MetadataChangeProposalClass(
            entityType="dataProduct",
            entityUrn=self.get_data_product_urn(),
            changeType=ChangeTypeClass.UPSERT,
            aspectName="esDataProductSchema",
            aspect=GenericAspectClass(
                contentType="application/json",
                value=json.dumps(
                    {
                        "esDataProductSchemaProperties": [
                            p.dict(by_alias=True)
                            for p in self.get_data_product_schema_properties_patched()
                        ]
                    }
                ).encode("utf-8"),
            ),
        )

    def generate_domain_mcp_if_not_exists(
        self, graph: DataHubGraph
    ) -> Iterable[Union[MetadataChangeProposalWrapper, MetadataChangeEventClass]]:
        domain_urn = self.get_data_product_domain_urn()
        if not graph.exists(domain_urn):
            yield MetadataChangeProposalWrapper(
                entityUrn=domain_urn,
                aspect=DomainPropertiesClass(name=self.get_data_product_domain()),
            )

    def generate_datasets_mcps_if_not_exists(
        self, graph: DataHubGraph
    ) -> Iterable[Union[MetadataChangeProposalWrapper, MetadataChangeEventClass]]:

        for dataset in self.output_ports:
            dataset_urn = dataset.get_urn()
            if not graph.exists(dataset_urn):
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DatasetPropertiesClass(
                        name=dataset.get_fully_qualified_name(),
                        customProperties=self.get_data_product_custom_properties(),
                    ),
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=SchemaMetadataClass(
                        schemaName=f"schema_{dataset.get_fully_qualified_name()}",
                        platform=make_data_platform_urn("databricks"),
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema=""),
                        fields=[
                            SchemaFieldClass(
                                fieldPath=prop.name,
                                type=SchemaFieldDataType(type=NullTypeClass()),
                                description="",
                                nativeDataType=prop.type,
                                glossaryTerms=GlossaryTermsClass(
                                    terms=[
                                        GlossaryTermAssociationClass(
                                            urn=prop.get_term_urn()
                                        )
                                    ],
                                    auditStamp=self._mint_auditstamp("json"),
                                ),
                            )
                            for prop in self.get_data_product_schema_properties()
                        ],
                    ),
                )

        for prop in self.get_data_product_schema_properties():
            term = prop.get_term_name()
            term_urn = prop.get_term_urn()
            if not graph.exists(term_urn):

                yield MetadataChangeEventClass(
                    proposedSnapshot=GlossaryTermSnapshotClass(
                        urn=term_urn,
                        aspects=[
                            GlossaryTermInfoClass(
                                name=term,
                                definition="*PUT THE DEFINITION HERE*, **you can use** MD markup language",
                                termSource="*PUT THE DEFINITION HERE*, **you can use** MD markup language",
                            ),
                            BrowsePathsClass(paths=prop.get_term_browse_path()),
                        ],
                    )
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=term_urn,
                    aspect=DomainsClass(
                        domains=[
                            self.get_data_product_domain_urn()
                        ]  # TODO: resolve it first
                    ),
                )
