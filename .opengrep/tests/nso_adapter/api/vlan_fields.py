class SubinterfaceEntry(BaseModel):
    # ruleid: nso-api-vlan-id-type
    dot1q_vlan: int


class VlanEntry(BaseModel):
    # ruleid: nso-api-vlan-id-type
    vlan_id: int


class SwitchportApply(BaseModel):
    # ruleid: nso-api-vlan-id-type
    untagged_vlan: Uint16 | None = None
    # ruleid: nso-api-vlan-id-type
    tagged_vlans: list[Uint16] = Field(default_factory=list)
    # ruleid: nso-api-vlan-id-type
    backup_vlan: int | None = None
    # ruleid: nso-api-vlan-id-type
    other_vlans: list[int] = Field(default_factory=list)
    # ok: nso-api-vlan-id-type
    primary_vlan: VlanId


class SwitchportOut(BaseModel):
    # ok: nso-api-vlan-id-type
    untagged_vlan: int | None
    # ok: nso-api-vlan-id-type
    tagged_vlans: list[int]
