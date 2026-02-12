CREATE OR REPLACE FORCE EDITIONABLE VIEW "CUSTOMERDATA"."V_CR_INSPECT_TO_SEND" (
    "INSPECTION_TYPE",
    "PIP_TYPE",
    "PROJECT_SID",
    "INSPECTIONID",
    "WORKORDER",
    "PROJECT",
    "PO_NUMBER",
    "ADDITIONAL_INFO",
    "PIPE_SEGMENT_REFERENCE",
    "LATERAL_SEGMENT_REFERENCE",
    "MANHOLE_NUMBER",
    "MATERIAL",
    "PIPE_USE",
    "COVER_SHAPE",
    "UPSTREAM_MH",
    "DOWNSTREAM_MH",
    "FACILITY_TYPE",
    "FACILITY_ID",
    "FACILITYOI",
    "SHAPE",
    "ACCESS_TYPE",
    "MH_USE",
    "WALL_MATERIAL",
    "BENCH_MATERIAL",
    "CHANNEL_MATERIAL",
    "WALL_BYSIZE",
    "WALL_DEPTH",
    "ELEVATION",
    "FRAME_MATERIAL",
    "HEIGHT",
    "UP_ELEVATION",
    "UP_GRADE_TO_INVERT",
    "DOWN_ELEVATION",
    "DOWN_GRADE_TO_INVERT",
    "STREET",
    "CITY",
    "TOTAL_LENGTH",
    "YEAR_CONSTRUCTED",
    "SIZE",
    "DRAINAGE_AREA",
    "FEED_STATUS"
) AS
    SELECT
        c.inspection_type,
        c.pip_type,
        c.project_sid,
        c.inspectionid,
        c.workorder,
        c.project,
        c.po_number,
  /* No cleanup/transformation; passthrough */
        c.additional_information,
        c.pipe_segment_reference,
        c.lateral_segment_reference,
        c.manhole_number,
        c.material,
        c.pipe_use,
        c.cover_shape,
        c.upstream_mh,
        c.downstream_mh,

  /* Extended set */
        c.facility_type,
        c.facility_id,
        c.facilityoi,
        c.shape,
        c.access_type,
        c.mh_use,
        c.wall_material,
        c.bench_material,
        c.channel_material,
        c.wall_bysize,
        c.wall_depth,
        c.elevation,
        c.frame_material,
        c.height,
        c.up_elevation,
        c.up_grade_to_invert,
        c.down_elevation,
        c.down_grade_to_invert,
        c.street,
        'Edmonton' AS city,
        c.total_length,
        c.year_constructed,
        c."SIZE",
        c.drainage_area,
        c.feed_status
    FROM
        customerdata.epsewerai_cr_inspect c
    WHERE
        upper(
            TRIM(c.feed_status)
        ) = 'NEW'
        AND c.inspection_type IS NOT NULL
        AND EXISTS (
            SELECT
                1
            FROM
                customerdata.epsewerai_wot_stg1 w
            WHERE
                upper(
                    TRIM(w.feed_status)
                ) = 'SENT'
      /* normalized UUID compare (strings with hyphens in table; strip for match) */
                AND replace(
                    upper(
                        TRIM(w.task_uuid)
                    ), '-', ''
                ) = replace(
                    upper(
                        TRIM(c.project_sid)
                    ), '-', ''
                )
        );