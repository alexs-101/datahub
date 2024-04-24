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

from datahub.api.entities.dataproduct.es_dataproduct import _EsDataProduct
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

def mutate(file: Path) -> None:
    data_product = _EsDataProduct.load_from_json(file)
   
    with get_default_graph() as graph:
        for mcp in data_product.generate_mcp(graph):
            logger.info(mcp)
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
def upsert(file: Path) -> None:
    mutate(file)