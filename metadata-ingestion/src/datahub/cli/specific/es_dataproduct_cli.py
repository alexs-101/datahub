import difflib
import json
import logging
import os
import pathlib
import sys
from pathlib import Path
from shutil import copyfile
from tempfile import NamedTemporaryFile
from typing import Optional


import click
from click_default_group import DefaultGroup

from datahub.api.entities.dataproduct.es_dataproduct import EsDataProduct
from datahub.cli.specific.file_loader import load_file
from datahub.metadata.schema_classes import OwnershipTypeClass
from datahub.specific.es_dataproduct import EsDataProductPatchBuilder
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

from datahub.cli.specific import dataproduct_cli

from pydantic import BaseModel, Extra, ValidationError, Field

logger = logging.getLogger(__name__)

def mutate(file: Path, validate_assets: bool, external_url: str, upsert: bool) -> None:
    data_product = EsDataProduct.from_es_json(file)

    with NamedTemporaryFile(suffix=".yaml") as f:
        yamlFile = Path(f.name)
        data_product.to_yaml(yamlFile)
        dataproduct_cli.mutate(yamlFile, validate_assets, external_url, upsert=upsert)
    
    with get_default_graph() as graph:
        for mcp in data_product.generate_es_mcp(upsert):
            graph.emit(mcp)


@click.group(cls=DefaultGroup, default="upsert")
def es_dataproduct() -> None:
    """A group of commands to interact with the Exact Science DataProduct entity in DataHub."""
    pass


@es_dataproduct.command(
    name="upsert",
    help="Upsert attributes to a Data Product in DataHub."
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@click.option("--external-url", required=False, type=str)
def upsert(file: Path, validate_assets: bool, external_url: str) -> None:
    mutate(file, validate_assets, external_url, upsert=True)