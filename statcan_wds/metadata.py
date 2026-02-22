from .client import get, post
from .errors import MetadataError


def get_cube_metatdata(pid):
    payload = [{"productId": pid}]
    data = post("getCubeMetadata", payload)

    if not isinstance(data, list) or not data:
        raise MetadataError(f"Unexpected metadata payload: {data}")
    
    item = data[0]
    if item.get("status") != "SUCCESS":
        raise MetadataError(item.get("object"))
    
    return item["object"]


def inspect_dimensions(pid, metadata):
    """
    Return dimension metadata as:
    {
        dimensionName: {
            "position": int,
            "values": { memberName: memberId }
        }
    }
    """
    dims = metadata.get("dimension")
    if not dims:
        raise MetadataError(f"No dimensions found in metadata for {pid}")
    return {
        dim["dimensionNameEn"]: {
            "position": dim["dimensionPositionId"],
            "values": {m["memberNameEn"]: m["memberId"] for m in dim.get("member", [])},
        }
        for dim in dims
    }
