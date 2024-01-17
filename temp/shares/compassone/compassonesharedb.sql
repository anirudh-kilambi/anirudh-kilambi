-- HAPPY OR NOT

GRANT USAGE ON SCHEMA COMPASSONESHAREDB.HAPPYORNOT to SHARE COMPASSONE_CROTHALL_SHARE;

create or replace secure view COMPASSONESHAREDB.HAPPYORNOT.SURVEYS as
(select * from DATAOPS_SOURCE.HAPPYORNOT.SURVEYS WHERE FOLDER_ROOT_KEY = 162205);

GRANT SELECT ON VIEW COMPASSONESHAREDB.HAPPYORNOT.SURVEYS TO SHARE COMPASSONE_CROTHALL_SHARE;

create or replace secure view COMPASSONESHAREDB.HAPPYORNOT.FOLDERS as
(select * from DATAOPS_SOURCE.HAPPYORNOT.FOLDERS WHERE FOLDER_ROOT_KEY = 162205);

GRANT SELECT ON VIEW COMPASSONESHAREDB.HAPPYORNOT.FOLDERS TO SHARE COMPASSONE_CROTHALL_SHARE;

create or replace secure view COMPASSONESHAREDB.HAPPYORNOT.QUESTIONS as
(select * from DATAOPS_SOURCE.HAPPYORNOT.QUESTIONS);

GRANT SELECT ON VIEW COMPASSONESHAREDB.HAPPYORNOT.QUESTIONS TO SHARE COMPASSONE_CROTHALL_SHARE;

-- ORIGAMI

create or replace secure view compassonesharedb.origami.claims as 
(
select * from dataops_source.origami.claims
where cost_center_system_id = 1001 and sector in
(
    'Morrison Living Sector',
    'Morrison Community Living Sector',
    'Morrison Healthcare Sector',
    'MORRISON HEALTHCARE SECTOR MANAGER GROUP',
    'TOUCHPOINT SECTOR MGR GROUP',
    'Unidine Sector',
    'Crothall Sector',
    'CROTHALL'
)
);

grant usage on schema compassonesharedb.origami to share compassone_crothall_share;

grant select on view compassonesharedb.origami.claims to share compassone_crothall_share;
