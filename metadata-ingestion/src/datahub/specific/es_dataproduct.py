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
    TagAssociationClass as Tag
)

class EsDataProductPatchBuilder(DataProductPatchBuilder):
    pass