import pkg_resources
from grpc_tools import protoc
from pathlib import Path
from importlib import import_module


definition = """
class PlaceholderEnumtype(Enum):
    @staticmethod
    def pb2_class() -> type[pb2.PlaceholderEnumtype]:
        return pb2.PlaceholderEnumtype

    @classmethod
    def from_grpc(cls, el: pb2.PlaceholderEnumtype) -> "PlaceholderEnumtype":
        return cls(cls.pb2_class().Name(el))  # type: ignore

    def to_grpc(self) -> pb2.PlaceholderEnumtype:
        return pb2.PlaceholderEnumtype.Value(self.value)  # type: ignore

    def __eq__(self, other: object) -> bool:
        if isinstance(other, pb2.PlaceholderEnumtype):
            return self.value == self.from_grpc(other).value
        if isinstance(other, PlaceholderEnumtype):
            return self.value == other.value
        return False
"""


def generate_enums(
    title: str,
    output_dir: Path,
) -> None:
    """Generate enums for a given proto file."""
    module_name = f"{output_dir.name}.{title}_pb2"

    file_content = f"""
# Generated code from {__file__}
# don't be editing you hear?
from enum import Enum

import {module_name} as pb2
    """.strip()
    pb2 = import_module(module_name)
    for el in dir(pb2):
        item_name = str(el)
        item = getattr(pb2, item_name)
        if not item or "EnumTypeWrapper" not in repr(item):
            continue

        new_def = definition.strip().replace("PlaceholderEnumtype", item_name) + "\n"
        for key in item.keys():  # noqa: SIM118
            new_def = new_def + f"""\n    {key} = "{key}" """[:-1]

        file_content = file_content + "\n\n\n" + new_def

    with open(output_dir / f"{title}_enums.py", "w") as wf:
        wf.write(file_content + "\n")


def run_generation(output_dir: Path, proto_path: Path) -> None:
    """Run the bash script to generate the grpc files."""
    proto_include = pkg_resources.resource_filename("grpc_tools", "_proto")
    args = [
        __file__,
        "-I",
        "/src/protos",
        "--python_out",
        str(output_dir),
        "--pyi_out",
        str(output_dir),
        str(proto_path),
        f"-I{proto_include}",
    ]
    return protoc.main(args)


def main() -> None:
    output_dir = Path("/src/generated_grpc")
    proto_path = Path("/src/protos/orca.proto")
    title = proto_path.stem
    output_dir.mkdir(exist_ok=True, parents=True)
    if run_generation(output_dir, proto_path):
        print("Failed to generate grpc files")
        return
    print("Generated grpc files")
    generate_enums(title, output_dir)
    print("Generated enums")


if __name__ == "__main__":
    main()
