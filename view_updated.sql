CREATE OR REPLACE FORCE EDITIONABLE VIEW "CUSTOMERDATA"."SEWERAI_INSPECTIONS_V" (
    "WORK_ORDERS_NUMBER",
    "WORK_ORDER_TASK_TITLE",
    "ASSET_NUMBER",
    "ADDITIONAL_INFORMATION",
    "FACILITYOI",
    "FACILITY_ID",
    "FACILITY_TYPE",
    "EPDRFACILITY_OI",
    "EPDRFACILITYWORKHISTORYOI",
    "CREATEDATE_DTTM",
    "LASTUPDATE_DTTM",
    "INSPECTION_TYPE",
    "PIP_TYPE",
    "PIPE_USE",
    "MATERIAL",
    "SHAPE",
    "ACCESS_TYPE",
    "MANHOLE_NUMBER",
    "MH_USE",
    "COVER_SHAPE",
    "WALL_MATERIAL",
    "BENCH_MATERIAL",
    "CHANNEL_MATERIAL",
    "WALL_BYSIZE",
    "WALL_DEPTH",
    "ELEVATION",
    "FRAME_MATERIAL",
    "PIPE_SEGMENT_REFERENCE",
    "LATERAL_SEGMENT_REFERENCE",
    "UPSTREAM_MH",
    "DOWNSTREAM_MH",
    "WORK_ORDER_TASK_UUID",
    "WORK_ORDER_UUID",
    "DR_UUID",
    "UNKNOWN_TYPE",
    "HEIGHT",
    "UP_ELEVATION",
    "UP_GRADE_TO_INVERT",
    "DOWN_ELEVATION",
    "DOWN_GRADE_TO_INVERT",
    "STREET",
    "TOTAL_LENGTH",
    "YEAR_CONSTRUCTED",
    "SIZE",
    "DRAINAGE_AREA"
) AS
WITH
/* ---------- Lookups used by MACP ---------- */
mh_use_map AS (
    SELECT 'COMBINED' AS ivara_mh_use, 'CB' AS pioneers_code FROM dual
    UNION ALL SELECT 'FOUNDATION DRAIN', 'ZZ' FROM dual
    UNION ALL SELECT 'NOT APPLICABLE', 'ZZ' FROM dual
    UNION ALL SELECT 'SANITARY', 'SS' FROM dual
    UNION ALL SELECT 'STORM', 'SW' FROM dual
),

/* ---------- Shared CLOB-safe HTML cleaner (materialize once) ---------- */
clean_longdesc AS (
    SELECT /*+ MATERIALIZE */
        a.workordertaskoi,
        REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(
                                                            REPLACE(
                                                                TO_CLOB(a.longdescript),
                                                                CHR(38) || 'amp;lt;',   CHR(38) || 'lt;'
                                                            ),
                                                            CHR(38) || 'amp;gt;',     CHR(38) || 'gt;'
                                                        ),
                                                        CHR(38) || 'amp;nbsp;',   CHR(38) || 'nbsp;'
                                                    ),
                                                    CHR(38) || 'amp;quot;',   CHR(38) || 'quot;'
                                                ),
                                                CHR(38) || 'amp;#39;',    CHR(38) || '#39;'
                                            ),
                                            CHR(38) || 'amp;amp;',    CHR(38) || 'amp;'
                                        ),
                                        '&lt;!DOCTYPE[^&gt;]*&gt;', '', 1, 0, 'in'
                                    ),
                                    '&lt;head[^&gt;]*&gt;.*?&lt;/head&gt;', '', 1, 0, 'in'
                                ),
                                '&lt;style[^&gt;]*&gt;.*?&lt;/style&gt;', '', 1, 0, 'in'
                            ),
                            '&lt;script[^&gt;]*&gt;.*?&lt;/script&gt;', '', 1, 0, 'in'
                        ),
                        '&lt;[^&gt;]+&gt;', '', 1, 0, 'n'
                    ),
                    '\s+', ' '
                ),
                '^\s+|\s+$', ''
            ),
            CHR(38) || 'amp;', CHR(38)
        ) AS additional_information_clob
    FROM
        mnt.workordertask a
),

/* ============================ MACP (Manhole & Catch Basin) ============================ */
macp_base AS (
    SELECT
        wo.wonumber || '.' || a.tasknumber                 AS work_orders_number,
        a.wotasktitle                                      AS work_order_task_title,
        s.assetnumber                                      AS asset_number,
        cli.additional_information_clob                    AS additional_information,
        e1.epdrdrainagefacilityoi                          AS facilityoi,
        e1.facilityid                                      AS facility_id,
        e1.facilitytype                                    AS facility_type,
        e.epdrfacility_oi,
        e.epdrfacilityworkhistoryoi,
        e.createdate_dttm,
        e.lastupdate_dttm,
        CASE
            WHEN e1.facilitytype = 10 AND ep2.unknfactype = 3 THEN 'AMH'
            WHEN e1.facilitytype = 10 AND ep2.unknfactype = 2 THEN 'ACB'
            WHEN e1.facilitytype = 1 THEN 'AMH'
            WHEN e1.facilitytype = 2 THEN 'ACB'
            ELSE NULL
        END                                                AS access_type,
        CASE
            WHEN e1.facilitytype = 1 THEN REGEXP_REPLACE(e1.facilityid, '^MH', '')
            WHEN e1.facilitytype = 2 THEN REGEXP_REPLACE(e1.facilityid, '^CB', '')
            WHEN e1.facilitytype = 10 AND ep2.unknfactype = 3 THEN REGEXP_REPLACE(e1.facilityid, '^MH', '')
            WHEN e1.facilitytype = 10 AND ep2.unknfactype = 2 THEN REGEXP_REPLACE(e1.facilityid, '^CB', '')
            ELSE NULL
        END                                                AS manhole_number,
        a.uuid                                             AS work_order_task_uuid,
        wo.uuid                                            AS work_order_uuid,
        e.uuid                                             AS dr_uuid,
        ep2.unknfactype                                    AS unknown_type,
        CASE
            WHEN e1.facilitytype IN (1, 2) THEN 'MACP'
            WHEN e1.facilitytype = 10 AND ep2.unknfactype IN (2, 3) THEN 'MACP'
            ELSE NULL
        END                                                AS inspection_type,

        /* Manhole attributes */
        mh.wwtype                                          AS mh_wwtype,
        mh.neighbourhd                                     AS mh_neighbourhd,
        mh.location                                        AS mh_location,
        mh.cone                                            AS mh_cone,
        mh.bench                                           AS mh_bench,
        mh.channel                                         AS mh_channel,
        mh.shape                                           AS mh_shape,
        mh.diameter_fl                                     AS mh_diameter_fl,
        mh.depth_fl                                        AS mh_depth_fl,
        mh.groundelevat_fl                                 AS mh_groundelevat_fl,

        /* Catch basin attributes */
        cb.wwtype                                          AS cb_wwtype,
        cb.neighbourhd                                     AS cb_neighbourhd,
        cb.location                                        AS cb_location,
        cb.shape                                           AS cb_shape,
        cb.diameter_fl                                     AS cb_diameter_fl,
        cb.depth_fl                                        AS cb_depth_fl,
        cb.framecover                                      AS cb_framecover,

        /* MACP-only reference (for CB / Unknown 2/3) */
        CASE
            WHEN (e1.facilitytype = 2 OR (e1.facilitytype = 10 AND ep2.unknfactype IN (2, 3))) THEN e1.facilityid
            ELSE NULL
        END                                                AS macp_pipe_segment_reference
    FROM
        mnt.workordertask a
        LEFT JOIN mnt.workorders wo                 ON a.workorder_oi = wo.workordersoi
        LEFT JOIN mnt.asset s                       ON a.asset_oi = s.assetoi
        JOIN customerdata.epdrfacworkhistory e      ON e.wotask_oi = a.workordertaskoi
        LEFT JOIN customerdata.epdrdrainfacility e1 ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi
        LEFT JOIN customerdata.epdrunknfac ep2      ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi
        LEFT JOIN customerdata.epdrmanhole mh       ON mh.manholeid = e1.facilityid
        LEFT JOIN customerdata.epdrcatchbasin cb    ON cb.catchbasinid = e1.facilityid
        LEFT JOIN clean_longdesc cli                ON cli.workordertaskoi = a.workordertaskoi
    WHERE
        (e1.facilitytype IN (1, 2)
         OR (e1.facilitytype = 10 AND ep2.unknfactype IN (2, 3)))
),

/* ============================ PACP (Pipes) ============================ */
pacp_base AS (
    SELECT
        wo.wonumber || '.' || a.tasknumber                 AS work_orders_number,
        a.wotasktitle                                      AS work_order_task_title,
        s.assetnumber                                      AS asset_number,
        cli.additional_information_clob                    AS additional_information,
        e1.epdrdrainagefacilityoi                          AS facilityoi,
        e1.facilityid                                      AS facility_id,
        e1.facilitytype                                    AS facility_type,
        e.epdrfacility_oi,
        e.epdrfacilityworkhistoryoi,
        e.createdate_dttm,
        e.lastupdate_dttm,
        ep1.material                                       AS raw_material,
        ep1.shape                                          AS raw_shape,
        ep1.wwtype                                         AS raw_wwtype,
        ep1.pipeid                                         AS raw_pipeid,
        ep1.usfacilityid                                   AS upstream_mh,
        ep1.dsfacilityid                                   AS downstream_mh,
        ep1.diameter_fl                                    AS raw_diameter_fl,
        ep1.usgroundelev_fl                                AS raw_usgroundelev_fl,
        ep1.usinvertelev_fl                                AS raw_usinvertelev_fl,
        ep1.dsgroundelev_fl                                AS raw_dsgroundelev_fl,
        ep1.dsinvertelev_fl                                AS raw_dsinvertelev_fl,
        ep1.location                                       AS raw_location,
        ep1.length_fl                                      AS raw_length_fl,
        ep1.yearconst                                      AS raw_yearconst,
        ep1.usneighbour                                    AS raw_usneighbour,
        CASE
            WHEN e1.facilitytype = 8 THEN 'PACP'
            WHEN e1.facilitytype = 10 AND ep2.unknfactype IN (4, 5, 6, 7, 8) THEN 'PACP'
            ELSE NULL
        END                                                AS inspection_type,
        'Pipe'                                             AS pip_type,
        CASE
            WHEN ep1.pipeid IS NULL THEN NULL
            WHEN UPPER(ep1.pipeid) LIKE 'PIP%' THEN SUBSTR(ep1.pipeid, 4)
            WHEN UPPER(ep1.pipeid) LIKE 'CBL%' THEN SUBSTR(ep1.pipeid, 4)
            ELSE ep1.pipeid
        END                                                AS base_pipe_ref,
        ep2.unknfactype                                    AS unknown_type,
        a.uuid                                             AS work_order_task_uuid,
        wo.uuid                                            AS work_order_uuid,
        e.uuid                                             AS dr_uuid
    FROM
        mnt.workordertask a
        LEFT JOIN mnt.workorders wo                 ON a.workorder_oi = wo.workordersoi
        LEFT JOIN mnt.asset s                       ON a.asset_oi = s.assetoi
        JOIN customerdata.epdrfacworkhistory e      ON e.wotask_oi = a.workordertaskoi
        LEFT JOIN customerdata.epdrdrainfacility e1 ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi
        LEFT JOIN customerdata.epdrpipe ep1         ON e1.epdrpipe_oi = ep1.epdrpipeoi
        LEFT JOIN customerdata.epdrunknfac ep2      ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi
        LEFT JOIN clean_longdesc cli                ON cli.workordertaskoi = a.workordertaskoi
    WHERE
        e1.facilitytype = 8
        OR (e1.facilitytype = 10 AND ep2.unknfactype IN (4, 5, 6, 7, 8))
),

/* ============================ LACP (Service Connections) ============================ */
lacp_base AS (
    SELECT
        wo.wonumber || '.' || a.tasknumber                 AS work_orders_number,
        a.wotasktitle                                      AS work_order_task_title,
        s.assetnumber                                      AS asset_number,
        cli.additional_information_clob                    AS additional_information,
        e1.epdrdrainagefacilityoi                          AS facilityoi,
        e1.facilityid                                      AS facility_id,
        e1.facilitytype                                    AS facility_type,
        e.epdrfacility_oi,
        e.epdrfacilityworkhistoryoi,
        e.createdate_dttm,
        e.lastupdate_dttm,
        esc.pipelength                                     AS lacp_pipelength,
        esc.pipesize                                       AS lacp_pipesize,
        esc.recordtype                                     AS lacp_recordtype,
        esc.location                                       AS lacp_location,
        esc.neighbourhd                                    AS lacp_neighbourhd,
        esc.pipetype                                       AS lacp_pipetype,
        esc.wass_appid                                     AS lacp_wass_appid,
        'LACP'                                             AS inspection_type,
        'Service Connection'                               AS pip_type,
        CASE
            WHEN INSTR(esc.wass_appid, '-', -1) > 1 THEN SUBSTR(esc.wass_appid, 1, INSTR(esc.wass_appid, '-', -1) - 1)
            ELSE esc.wass_appid
        END                                                AS base_pipe_ref,
        ep2.unknfactype                                    AS unknown_type,
        a.uuid                                             AS work_order_task_uuid,
        wo.uuid                                            AS work_order_uuid,
        e.uuid                                             AS dr_uuid
    FROM
        mnt.workordertask a
        LEFT JOIN mnt.workorders wo                         ON a.workorder_oi = wo.workordersoi
        LEFT JOIN mnt.asset s                               ON a.asset_oi = s.assetoi
        JOIN customerdata.epdrfacworkhistory e              ON e.wotask_oi = a.workordertaskoi
        LEFT JOIN customerdata.epdrdrainfacility e1         ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi
        LEFT JOIN customerdata."EPDRSERVICECONNECT" esc     ON esc.wass_appid = e1.facilityid
        LEFT JOIN customerdata.epdrunknfac ep2              ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi
        LEFT JOIN clean_longdesc cli                        ON cli.workordertaskoi = a.workordertaskoi
    WHERE
        e1.facilitytype = 4
        OR (e1.facilitytype = 10 AND ep2.unknfactype IN (6, 7))
),

/* ============================ Normalize each branch (typed) ============================ */
macp_out AS (
    SELECT
        CAST(b.work_orders_number AS VARCHAR2(100))          AS work_orders_number,
        CAST(b.work_order_task_title AS VARCHAR2(400))       AS work_order_task_title,
        CAST(b.asset_number AS VARCHAR2(200))                AS asset_number,
        TO_NCLOB(b.additional_information)                   AS additional_information,
        CAST(b.facilityoi AS NUMBER)                         AS facilityoi,
        CAST(b.facility_id AS VARCHAR2(200))                 AS facility_id,
        CAST(b.facility_type AS NUMBER)                      AS facility_type,
        CAST(b.epdrfacility_oi AS NUMBER)                    AS epdrfacility_oi,
        CAST(b.epdrfacilityworkhistoryoi AS NUMBER)          AS epdrfacilityworkhistoryoi,
        CAST(b.createdate_dttm AS DATE)                      AS createdate_dttm,
        CAST(b.lastupdate_dttm AS DATE)                      AS lastupdate_dttm,
        CAST(b.inspection_type AS VARCHAR2(20))              AS inspection_type,
        CAST(NULL AS VARCHAR2(200))                          AS pip_type,
        CAST(NULL AS VARCHAR2(50))                           AS pipe_use,
        CAST(NULL AS VARCHAR2(200))                          AS material,
        CAST(NULL AS VARCHAR2(20))                           AS shape,
        CAST(b.access_type AS VARCHAR2(50))                  AS access_type,
        CAST(b.manhole_number AS VARCHAR2(200))              AS manhole_number,
        CAST(mhuse_map.pioneers_code AS VARCHAR2(50))        AS mh_use,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN sc_cb.pioneers_code
                ELSE sc_mh.pioneers_code
            END
            AS VARCHAR2(20)
        )                                                    AS cover_shape,
        CAST(
            CASE
                WHEN b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)) THEN NULL
                ELSE mat_wall.pioneers_code
            END
            AS VARCHAR2(20)
        )                                                    AS wall_material,
        CAST(
            CASE
                WHEN b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)) THEN NULL
                ELSE mat_bench.pioneers_code
            END
            AS VARCHAR2(20)
        )                                                    AS bench_material,
        CAST(
            CASE
                WHEN b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)) THEN NULL
                ELSE mat_chan.pioneers_code
            END
            AS VARCHAR2(20)
        )                                                    AS channel_material,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_diameter_fl
                ELSE b.mh_diameter_fl
            END
            AS NUMBER
        )                                                    AS wall_bysize,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_depth_fl
                ELSE b.mh_depth_fl
            END
            AS NUMBER
        )                                                    AS wall_depth,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN NULL
                ELSE b.mh_groundelevat_fl
            END
            AS NUMBER
        )                                                    AS elevation,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_framecover
                ELSE NULL
            END
            AS VARCHAR2(100)
        )                                                    AS frame_material,
        CAST(b.macp_pipe_segment_reference AS VARCHAR2(200)) AS pipe_segment_reference,
        CAST(NULL AS VARCHAR2(200))                          AS lateral_segment_reference,
        CAST(NULL AS VARCHAR2(200))                          AS upstream_mh,
        CAST(NULL AS VARCHAR2(200))                          AS downstream_mh,
        CAST(b.work_order_task_uuid AS VARCHAR2(100))        AS work_order_task_uuid,
        CAST(b.work_order_uuid AS VARCHAR2(100))             AS work_order_uuid,
        CAST(b.dr_uuid AS VARCHAR2(100))                     AS dr_uuid,
        CAST(b.unknown_type AS NUMBER)                       AS unknown_type,
        CAST(NULL AS NUMBER)                                 AS height,
        CAST(NULL AS NUMBER)                                 AS up_elevation,
        CAST(NULL AS NUMBER)                                 AS up_grade_to_invert,
        CAST(NULL AS NUMBER)                                 AS down_elevation,
        CAST(NULL AS NUMBER)                                 AS down_grade_to_invert,
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_location
                ELSE b.mh_location
            END
            AS VARCHAR2(400)
        )                                                    AS street,
        CAST(NULL AS NUMBER)                                 AS total_length,
        CAST(NULL AS NUMBER)                                 AS year_constructed,
        CAST(NULL AS VARCHAR2(100))                          AS "SIZE",
        CAST(
            CASE
                WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_neighbourhd
                ELSE b.mh_neighbourhd
            END
            AS VARCHAR2(200)
        )                                                    AS drainage_area
    FROM
        macp_base b
        LEFT JOIN customerdata.epsewerai_shape_code sc_mh
            ON UPPER(TRIM(sc_mh.ivara_shape)) = UPPER(TRIM(b.mh_shape))
        LEFT JOIN customerdata.epsewerai_shape_code sc_cb
            ON UPPER(TRIM(sc_cb.ivara_shape)) = UPPER(TRIM(b.cb_shape))
        LEFT JOIN (
            SELECT UPPER(TRIM(ivara_mh_use)) AS ivara_mh_use, pioneers_code FROM mh_use_map
        ) mhuse_map
            ON UPPER(TRIM(
                CASE
                    WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_wwtype
                    ELSE b.mh_wwtype
                END
            )) = mhuse_map.ivara_mh_use
        LEFT JOIN customerdata.epsewerai_wall_bench_channel_code mat_wall
            ON UPPER(TRIM(mat_wall.ivara_material)) = UPPER(TRIM(
                CASE
                    WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))) THEN NULL
                    ELSE b.mh_cone
                END
            ))
        LEFT JOIN customerdata.epsewerai_wall_bench_channel_code mat_bench
            ON UPPER(TRIM(mat_bench.ivara_material)) = UPPER(TRIM(
                CASE
                    WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))) THEN NULL
                    ELSE b.mh_bench
                END
            ))
        LEFT JOIN customerdata.epsewerai_wall_bench_channel_code mat_chan
            ON UPPER(TRIM(mat_chan.ivara_material)) = UPPER(TRIM(
                CASE
                    WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))) THEN NULL
                    ELSE b.mh_channel
                END
            ))
),

pacp_out AS (
    SELECT
        CAST(b.work_orders_number AS VARCHAR2(100))    AS work_orders_number,
        CAST(b.work_order_task_title AS VARCHAR2(400)) AS work_order_task_title,
        CAST(b.asset_number AS VARCHAR2(200))          AS asset_number,
        TO_NCLOB(b.additional_information)             AS additional_information,
        CAST(b.facilityoi AS NUMBER)                   AS facilityoi,
        CAST(b.facility_id AS VARCHAR2(200))           AS facility_id,
        CAST(b.facility_type AS NUMBER)                AS facility_type,
        CAST(b.epdrfacility_oi AS NUMBER)              AS epdrfacility_oi,
        CAST(b.epdrfacilityworkhistoryoi AS NUMBER)    AS epdrfacilityworkhistoryoi,
        CAST(b.createdate_dttm AS DATE)                AS createdate_dttm,
        CAST(b.lastupdate_dttm AS DATE)                AS lastupdate_dttm,
        CAST(b.inspection_type AS VARCHAR2(20))        AS inspection_type,
        CAST(b.pip_type AS VARCHAR2(200))              AS pip_type,
        CAST(
            CASE
                WHEN UPPER(TRIM(b.raw_wwtype)) = 'FOUNDATION' THEN 'PN'
                WHEN UPPER(TRIM(b.raw_wwtype)) = 'SANITARY'   THEN 'SS'
                WHEN UPPER(TRIM(b.raw_wwtype)) = 'STORM'      THEN 'SW'
                WHEN UPPER(TRIM(b.raw_wwtype)) = 'WATER'      THEN 'XX'
                WHEN UPPER(TRIM(b.raw_wwtype)) = 'COMBINED'   THEN 'CB'
                WHEN UPPER(TRIM(b.raw_wwtype)) IN ('NOT APPLICABLE', 'N/A', 'NA') THEN 'XX'
                ELSE NULL
            END
            AS VARCHAR2(50)
        )                                             AS pipe_use,
        CAST(COALESCE(mc.pioneers_code, b.raw_material) AS VARCHAR2(200)) AS material,
        CAST(sc.pioneers_code AS VARCHAR2(20))         AS shape,
        CAST(NULL AS VARCHAR2(50))                     AS access_type,
        CAST(NULL AS VARCHAR2(200))                    AS manhole_number,
        CAST(NULL AS VARCHAR2(50))                     AS mh_use,
        CAST(NULL AS VARCHAR2(20))                     AS cover_shape,
        CAST(NULL AS VARCHAR2(20))                     AS wall_material,
        CAST(NULL AS VARCHAR2(20))                     AS bench_material,
        CAST(NULL AS VARCHAR2(20))                     AS channel_material,
        CAST(NULL AS NUMBER)                           AS wall_bysize,
        CAST(NULL AS NUMBER)                           AS wall_depth,
        CAST(NULL AS NUMBER)                           AS elevation,
        CAST(NULL AS VARCHAR2(100))                    AS frame_material,
        CAST(
            CASE
                WHEN b.facility_type = 10 AND b.unknown_type IN (4, 5) THEN b.facility_id
                ELSE b.base_pipe_ref
            END
            AS VARCHAR2(200)
        )                                             AS pipe_segment_reference,
        CAST(NULL AS VARCHAR2(200))                    AS lateral_segment_reference,
        CAST(b.upstream_mh AS VARCHAR2(200))           AS upstream_mh,
        CAST(b.downstream_mh AS VARCHAR2(200))         AS downstream_mh,
        CAST(b.work_order_task_uuid AS VARCHAR2(100))  AS work_order_task_uuid,
        CAST(b.work_order_uuid AS VARCHAR2(100))       AS work_order_uuid,
        CAST(b.dr_uuid AS VARCHAR2(100))               AS dr_uuid,
        CAST(b.unknown_type AS NUMBER)                 AS unknown_type,
        CAST(b.raw_diameter_fl AS NUMBER)              AS height,
        CAST(b.raw_usgroundelev_fl AS NUMBER)          AS up_elevation,
        CAST(b.raw_usinvertelev_fl AS NUMBER)          AS up_grade_to_invert,
        CAST(b.raw_dsgroundelev_fl AS NUMBER)          AS down_elevation,
        CAST(b.raw_dsinvertelev_fl AS NUMBER)          AS down_grade_to_invert,
        CAST(b.raw_location AS VARCHAR2(400))          AS street,
        CAST(b.raw_length_fl AS NUMBER)                AS total_length,
        CAST(b.raw_yearconst AS NUMBER)                AS year_constructed,
        CAST(NULL AS VARCHAR2(100))                    AS "SIZE",
        CAST(b.raw_usneighbour AS VARCHAR2(200))       AS drainage_area
    FROM
        pacp_base b
        LEFT JOIN customerdata.epsewerai_material_code mc
            ON UPPER(TRIM(mc.ivara_material)) = UPPER(TRIM(b.raw_material))
        LEFT JOIN customerdata.epsewerai_shape_code sc
            ON UPPER(TRIM(sc.ivara_shape)) = UPPER(TRIM(b.raw_shape))
),

lacp_out AS (
    SELECT
        CAST(b.work_orders_number AS VARCHAR2(100))    AS work_orders_number,
        CAST(b.work_order_task_title AS VARCHAR2(400)) AS work_order_task_title,
        CAST(b.asset_number AS VARCHAR2(200))          AS asset_number,
        TO_NCLOB(b.additional_information)             AS additional_information,
        CAST(b.facilityoi AS NUMBER)                   AS facilityoi,
        CAST(b.facility_id AS VARCHAR2(200))           AS facility_id,
        CAST(b.facility_type AS NUMBER)                AS facility_type,
        CAST(b.epdrfacility_oi AS NUMBER)              AS epdrfacility_oi,
        CAST(b.epdrfacilityworkhistoryoi AS NUMBER)    AS epdrfacilityworkhistoryoi,
        CAST(b.createdate_dttm AS DATE)                AS createdate_dttm,
        CAST(b.lastupdate_dttm AS DATE)                AS lastupdate_dttm,
        CAST(b.inspection_type AS VARCHAR2(20))        AS inspection_type,
        CAST(b.pip_type AS VARCHAR2(200))              AS pip_type,
        CAST(
            CASE
                WHEN UPPER(TRIM(b.lacp_recordtype)) = 'FOUNDATION' THEN 'PN'
                WHEN UPPER(TRIM(b.lacp_recordtype)) = 'SANITARY'   THEN 'SS'
                WHEN UPPER(TRIM(b.lacp_recordtype)) = 'STORM'      THEN 'SW'
                WHEN UPPER(TRIM(b.lacp_recordtype)) = 'WATER'      THEN 'XX'
                WHEN UPPER(TRIM(b.lacp_recordtype)) IN ('NOT APPLICABLE', 'N/A', 'NA') THEN 'XX'
                ELSE NULL
            END
            AS VARCHAR2(50)
        )                                             AS pipe_use,
        CAST(COALESCE(mc_sc.pioneers_code, b.lacp_pipetype) AS VARCHAR2(200)) AS material,
        CAST(NULL AS VARCHAR2(20))                     AS shape,
        CAST(NULL AS VARCHAR2(50))                     AS access_type,
        CAST(NULL AS VARCHAR2(200))                    AS manhole_number,
        CAST(NULL AS VARCHAR2(50))                     AS mh_use,
        CAST(NULL AS VARCHAR2(20))                     AS cover_shape,
        CAST(NULL AS VARCHAR2(20))                     AS wall_material,
        CAST(NULL AS VARCHAR2(20))                     AS bench_material,
        CAST(NULL AS VARCHAR2(20))                     AS channel_material,
        CAST(NULL AS NUMBER)                           AS wall_bysize,
        CAST(NULL AS NUMBER)                           AS wall_depth,
        CAST(NULL AS NUMBER)                           AS elevation,
        CAST(NULL AS VARCHAR2(100))                    AS frame_material,
        CAST(NULL AS VARCHAR2(200))                    AS pipe_segment_reference,
        CAST(
            CASE
                WHEN b.facility_type = 10 AND b.unknown_type IN (6, 7) THEN b.facility_id
                ELSE b.base_pipe_ref
            END
            AS VARCHAR2(200)
        )                                             AS lateral_segment_reference,
        CAST(NULL AS VARCHAR2(200))                    AS upstream_mh,
        CAST(NULL AS VARCHAR2(200))                    AS downstream_mh,
        CAST(b.work_order_task_uuid AS VARCHAR2(100))  AS work_order_task_uuid,
        CAST(b.work_order_uuid AS VARCHAR2(100))       AS work_order_uuid,
        CAST(b.dr_uuid AS VARCHAR2(100))               AS dr_uuid,
        CAST(b.unknown_type AS NUMBER)                 AS unknown_type,
        CAST(NULL AS NUMBER)                           AS height,
        CAST(NULL AS NUMBER)                           AS up_elevation,
        CAST(NULL AS NUMBER)                           AS up_grade_to_invert,
        CAST(NULL AS NUMBER)                           AS down_elevation,
        CAST(NULL AS NUMBER)                           AS down_grade_to_invert,
        CAST(b.lacp_location AS VARCHAR2(400))         AS street,
        CAST(b.lacp_pipelength AS NUMBER)              AS total_length,
        CAST(NULL AS NUMBER)                           AS year_constructed,
        CAST(b.lacp_pipesize AS VARCHAR2(100))         AS "SIZE",
        CAST(b.lacp_neighbourhd AS VARCHAR2(200))      AS drainage_area
    FROM
        lacp_base b
        LEFT JOIN customerdata.epsewerai_material_code mc_sc
            ON UPPER(TRIM(mc_sc.ivara_material)) = UPPER(TRIM(b.lacp_pipetype))
)

SELECT
    "WORK_ORDERS_NUMBER",
    "WORK_ORDER_TASK_TITLE",
    "ASSET_NUMBER",
    "ADDITIONAL_INFORMATION",
    "FACILITYOI",
    "FACILITY_ID",
    "FACILITY_TYPE",
    "EPDRFACILITY_OI",
    "EPDRFACILITYWORKHISTORYOI",
    "CREATEDATE_DTTM",
    "LASTUPDATE_DTTM",
    "INSPECTION_TYPE",
    "PIP_TYPE",
    "PIPE_USE",
    "MATERIAL",
    "SHAPE",
    "ACCESS_TYPE",
    "MANHOLE_NUMBER",
    "MH_USE",
    "COVER_SHAPE",
    "WALL_MATERIAL",
    "BENCH_MATERIAL",
    "CHANNEL_MATERIAL",
    "WALL_BYSIZE",
    "WALL_DEPTH",
    "ELEVATION",
    "FRAME_MATERIAL",
    "PIPE_SEGMENT_REFERENCE",
    "LATERAL_SEGMENT_REFERENCE",
    "UPSTREAM_MH",
    "DOWNSTREAM_MH",
    "WORK_ORDER_TASK_UUID",
    "WORK_ORDER_UUID",
    "DR_UUID",
    "UNKNOWN_TYPE",
    "HEIGHT",
    "UP_ELEVATION",
    "UP_GRADE_TO_INVERT",
    "DOWN_ELEVATION",
    "DOWN_GRADE_TO_INVERT",
    "STREET",
    "TOTAL_LENGTH",
    "YEAR_CONSTRUCTED",
    "SIZE",
    "DRAINAGE_AREA"
FROM (
    SELECT * FROM macp_out
    UNION ALL
    SELECT * FROM pacp_out
    UNION ALL
    SELECT * FROM lacp_out
);
