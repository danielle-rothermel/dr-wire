from __future__ import annotations

import datetime
import tomllib
from collections import Counter
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

import dr_http

DEFS_DIRECTORY = Path(__file__).resolve().parents[1] / ".defs"
NonEmptyText = Annotated[str, Field(min_length=1)]


class Term(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: NonEmptyText
    definition: NonEmptyText
    exported_symbols: tuple[NonEmptyText, ...] = ()
    categories: tuple[NonEmptyText, ...] = ()
    is_a: tuple[NonEmptyText, ...] = ()
    part_of: tuple[NonEmptyText, ...] = ()


class TermsDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    terms: tuple[Term, ...]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: NonEmptyText
    statement: NonEmptyText
    rationale: NonEmptyText
    date: NonEmptyText
    check: NonEmptyText | None = None


class ContractsDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    contracts: tuple[Contract, ...]


def load_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as file:
        return tomllib.load(file)


def duplicate_values(values: list[str]) -> list[str]:
    return sorted(
        value for value, count in Counter(values).items() if count > 1
    )


def find_relationship_cycle(terms: tuple[Term, ...]) -> tuple[str, ...] | None:
    graph = {term.name: (*term.is_a, *term.part_of) for term in terms}
    visiting: list[str] = []
    visited: set[str] = set()

    def visit(name: str) -> tuple[str, ...] | None:
        if name in visiting:
            cycle_start = visiting.index(name)
            return (*visiting[cycle_start:], name)
        if name in visited:
            return None

        visiting.append(name)
        for target in graph[name]:
            cycle = visit(target)
            if cycle is not None:
                return cycle
        visiting.pop()
        visited.add(name)
        return None

    for name in graph:
        cycle = visit(name)
        if cycle is not None:
            return cycle
    return None


def validate_relationships(document: TermsDocument) -> list[str]:
    errors: list[str] = []
    names = [term.name for term in document.terms]
    duplicate_names = duplicate_values(names)
    if duplicate_names:
        errors.append(f"duplicate term names: {', '.join(duplicate_names)}")

    known_names = set(names)
    for term in document.terms:
        for relationship in ("is_a", "part_of"):
            targets = getattr(term, relationship)
            duplicate_targets = duplicate_values(list(targets))
            if duplicate_targets:
                errors.append(
                    f"{term.name}.{relationship} has duplicate targets: "
                    f"{', '.join(duplicate_targets)}"
                )
            for target in targets:
                if target == term.name:
                    errors.append(
                        f"{term.name}.{relationship} links to itself"
                    )
                elif target not in known_names:
                    errors.append(
                        f"{term.name}.{relationship} has unknown target: "
                        f"{target}"
                    )

    if not errors:
        cycle = find_relationship_cycle(document.terms)
        if cycle is not None:
            errors.append(f"relationship cycle: {' -> '.join(cycle)}")

    return errors


def validate_exports(document: TermsDocument) -> list[str]:
    errors: list[str] = []
    mapped_symbols = [
        symbol for term in document.terms for symbol in term.exported_symbols
    ]
    duplicate_symbols = duplicate_values(mapped_symbols)
    if duplicate_symbols:
        errors.append(
            f"exports mapped more than once: {', '.join(duplicate_symbols)}"
        )

    public_symbols = list(dr_http.__all__)
    missing_symbols = sorted(set(public_symbols) - set(mapped_symbols))
    extra_symbols = sorted(set(mapped_symbols) - set(public_symbols))
    if missing_symbols:
        errors.append(f"unmapped public exports: {', '.join(missing_symbols)}")
    if extra_symbols:
        errors.append(f"mapped non-exports: {', '.join(extra_symbols)}")
    errors.extend(
        f"mapped export does not resolve: {symbol}"
        for symbol in mapped_symbols
        if not hasattr(dr_http, symbol)
    )

    return errors


def validate_terms(document: TermsDocument) -> list[str]:
    return [
        *validate_relationships(document),
        *validate_exports(document),
    ]


def validate_contracts(document: ContractsDocument) -> list[str]:
    errors: list[str] = []
    titles = [contract.title for contract in document.contracts]
    duplicate_titles = duplicate_values(titles)
    if duplicate_titles:
        errors.append(
            f"duplicate contract titles: {', '.join(duplicate_titles)}"
        )
    for contract in document.contracts:
        try:
            datetime.date.fromisoformat(contract.date)
        except ValueError:
            errors.append(f"contract has invalid ISO date: {contract.title}")
    return errors


def main() -> None:
    terms = TermsDocument.model_validate(
        load_toml(DEFS_DIRECTORY / "terms.toml")
    )
    contracts = ContractsDocument.model_validate(
        load_toml(DEFS_DIRECTORY / "contracts.toml")
    )
    errors = [*validate_terms(terms), *validate_contracts(contracts)]
    if errors:
        raise SystemExit("\n".join(f"- {error}" for error in errors))

    print(
        f"validated {len(terms.terms)} terms, "
        f"{len(dr_http.__all__)} exports, and "
        f"{len(contracts.contracts)} contracts"
    )


if __name__ == "__main__":
    main()
