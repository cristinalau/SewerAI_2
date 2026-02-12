create or replace TRIGGER IVARA.TRG_CR_INSPECT
FOR INSERT ON CUSTOMERDATA.EPDRFACWORKHISTORY
COMPOUND TRIGGER

  TYPE t_key IS RECORD ( dr_uuid VARCHAR2(50) );
  TYPE t_tab IS TABLE OF t_key;
  g_rows t_tab;

  BEFORE STATEMENT IS
  BEGIN
    g_rows := t_tab();
  END BEFORE STATEMENT;

  BEFORE EACH ROW IS
  BEGIN
    g_rows.EXTEND;
    g_rows(g_rows.COUNT).dr_uuid := :NEW.uuid;
  END BEFORE EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    FOR i IN 1 .. g_rows.COUNT LOOP

      MERGE INTO CUSTOMERDATA.EPSEWERAI_CR_INSPECT t
      USING (
        SELECT
          /* === Canonicalize UUIDs to lowercase 8-4-4-4-12 strings === */
          LOWER(
            REGEXP_REPLACE(
              REGEXP_REPLACE(v2.WORK_ORDER_TASK_UUID, '-', ''),
              '^(........)(....)(....)(....)(............)$',
              '\1-\2-\3-\4-\5'
            )
          ) AS PROJECT_SID,

          LOWER(
            REGEXP_REPLACE(
              REGEXP_REPLACE(v2.DR_UUID, '-', ''),
              '^(........)(....)(....)(....)(............)$',
              '\1-\2-\3-\4-\5'
            )
          ) AS INSPECTIONID,

          /* === Core columns (apply truncations here) === */
          SUBSTR(v2.WORK_ORDERS_NUMBER,    1, 15)  AS WORKORDER,
          /* Workorder UUID from MNT.WORKORDERS.UUID */
          LOWER(
            REGEXP_REPLACE(
              REGEXP_REPLACE(v2.WORKORDER_UUID, '-', ''),
              '^(........)(....)(....)(....)(............)$',
              '\\1-\\2-\\3-\\4-\\5'
            )
          ) AS WORKORDER_UUID,

          SUBSTR(v2.WORK_ORDER_TASK_TITLE, 1, 503) AS PROJECT,
          SUBSTR(v2.ASSET_NUMBER,          1, 50)  AS PO_NUMBER,

          /* Already sanitized plain text */
          v2.CLEAN_INFO_STEP AS CLEAN_INFO,

          /* Segment refs with truncations */
          SUBSTR(v2.PIPE_SEGMENT_REFERENCE,    1, 15) AS PIPE_SEGMENT_REFERENCE,
          SUBSTR(v2.LATERAL_SEGMENT_REFERENCE, 1, 15) AS LATERAL_SEGMENT_REFERENCE,
          SUBSTR(v2.MANHOLE_NUMBER,            1, 15) AS MANHOLE_NUMBER,

          /* Codes/lookups */
          v2.MATERIAL,
          v2.PIPE_USE,
          NVL(v2.COVER_SHAPE, 'Z') AS COVER_SHAPE,

          /* MH ends (truncate here for consistency) */
          SUBSTR(v2.UPSTREAM_MH,   1, 15) AS UPSTREAM_MH,
          SUBSTR(v2.DOWNSTREAM_MH, 1, 15) AS DOWNSTREAM_MH,

          /* Extended set */
          v2.INSPECTION_TYPE,
          v2.FACILITY_TYPE,
          v2.FACILITY_ID,
          v2.FACILITYOI,
          v2.PIP_TYPE,
          v2.SHAPE,
          v2.ACCESS_TYPE,
          v2.MH_USE,
          v2.WALL_MATERIAL,
          v2.BENCH_MATERIAL,
          v2.CHANNEL_MATERIAL,
          v2.WALL_BYSIZE,
          v2.WALL_DEPTH,
          v2.ELEVATION,
          v2.FRAME_MATERIAL,
          v2.HEIGHT,
          v2.UP_ELEVATION,
          v2.UP_GRADE_TO_INVERT,
          v2.DOWN_ELEVATION,
          v2.DOWN_GRADE_TO_INVERT,
          v2.STREET,
          v2.TOTAL_LENGTH,
          v2.YEAR_CONSTRUCTED,
          v2."SIZE" AS SIZE_COL,
          v2.DRAINAGE_AREA,
          v2.UNKNOWN_TYPE,
          v2.CREATEDATE_DTTM,
          v2.LASTUPDATE_DTTM
        FROM (
          /* === Inner SELECT computes CLEAN_INFO_STEP (fully de-HTML'd) === */
          SELECT
            v.WORK_ORDER_TASK_UUID,
            v.DR_UUID,
            v.WORK_ORDERS_NUMBER,
            wo.UUID AS WORKORDER_UUID,
            v.WORK_ORDER_TASK_TITLE,
            v.ASSET_NUMBER,
            v.PIPE_SEGMENT_REFERENCE,
            v.LATERAL_SEGMENT_REFERENCE,
            v.MANHOLE_NUMBER,
            v.MATERIAL,
            v.PIPE_USE,
            v.COVER_SHAPE,
            v.UPSTREAM_MH,
            v.DOWNSTREAM_MH,
            v.INSPECTION_TYPE,
            v.FACILITY_TYPE,
            v.FACILITY_ID,
            v.FACILITYOI,
            v.PIP_TYPE,
            v.SHAPE,
            v.ACCESS_TYPE,
            v.MH_USE,
            v.WALL_MATERIAL,
            v.BENCH_MATERIAL,
            v.CHANNEL_MATERIAL,
            v.WALL_BYSIZE,
            v.WALL_DEPTH,
            v.ELEVATION,
            v.FRAME_MATERIAL,
            v.HEIGHT,
            v.UP_ELEVATION,
            v.UP_GRADE_TO_INVERT,
            v.DOWN_ELEVATION,
            v.DOWN_GRADE_TO_INVERT,
            v.STREET,
            v.TOTAL_LENGTH,
            v.YEAR_CONSTRUCTED,
            v."SIZE",
            v.DRAINAGE_AREA,
            v.UNKNOWN_TYPE,
            v.CREATEDATE_DTTM,
            v.LASTUPDATE_DTTM,

            /* ===== Sanitizer (decode entities ? strip tags/blocks ? normalize) =====
               1) Decode &lt;, &gt;, &quot;, &#39;, &nbsp;, &amp;
               2) Remove DOCTYPE, HEAD, STYLE, SCRIPT
               3) Remove all remaining tags
               4) Collapse whitespace and trim
            */
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          -- (1) Decode common entities
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(
                                      v.ADDITIONAL_INFORMATION,
                                      CHR(38)||'lt;',   '<'
                                    ),
                                      CHR(38)||'gt;',   '>'
                                  ),
                                      CHR(38)||'quot;', '"'
                                ),
                                      CHR(38)||'#39;',  ''''
                              ),
                                      CHR(38)||'nbsp;',' '
                            ),
                                      CHR(38)||'amp;',  CHR(38)  -- &amp; ? &
                          ),

                          -- (2a) Strip DOCTYPE
                          '<!DOCTYPE[^>]*>', '', 1, 0, 'in'
                        ),
                          -- (2b) Strip <head>...</head>
                          '<head[^>]*>.*?</head>', '', 1, 0, 'in'
                      ),
                          -- (2c) Strip <style>...</style>
                          '<style[^>]*>.*?</style>', '', 1, 0, 'in'
                    ),
                          -- (2d) Strip <script>...</script>
                          '<script[^>]*>.*?</script>', '', 1, 0, 'in'
                  ),
                    -- (3) Remove any remaining tags
                    '<[^>]+>', '', 1, 0, 'n'
                ),
                -- (4a) Collapse whitespace
                '\s+', ' '
              ),
              -- (4b) Trim start/end
              '^\s+|\s+$', ''
            ) AS CLEAN_INFO_STEP

          FROM CUSTOMERDATA.SEWERAI_INSPECTIONS_V v
          JOIN MNT.WORKORDERTASK a
            ON a.UUID = v.WORK_ORDER_TASK_UUID
          JOIN MNT.WORKORDERS wo
            ON wo.WORKORDERSOI = a.WORKORDER_OI
          WHERE v.DR_UUID = g_rows(i).dr_uuid
            AND wo.SITE_OI = 58
        ) v2
      ) s
      ON (t.PROJECT_SID = s.PROJECT_SID AND t.INSPECTIONID = s.INSPECTIONID)

      WHEN MATCHED THEN UPDATE SET
        t.WORKORDER                 = s.WORKORDER,
        t.WORKORDER_UUID            = s.WORKORDER_UUID,
        t.PROJECT                   = s.PROJECT,
        t.PO_NUMBER                 = s.PO_NUMBER,
        t.ADDITIONAL_INFORMATION    = s.CLEAN_INFO,
        t.PIPE_SEGMENT_REFERENCE    = s.PIPE_SEGMENT_REFERENCE,
        t.LATERAL_SEGMENT_REFERENCE = s.LATERAL_SEGMENT_REFERENCE,
        t.MANHOLE_NUMBER            = s.MANHOLE_NUMBER,
        t.MATERIAL                  = s.MATERIAL,
        t.PIPE_USE                  = s.PIPE_USE,
        t.COVER_SHAPE               = s.COVER_SHAPE,
        t.UPSTREAM_MH               = s.UPSTREAM_MH,
        t.DOWNSTREAM_MH             = s.DOWNSTREAM_MH,
        t.FEED_STATUS               = 'NEW',
        t.FACILITY_TYPE             = s.FACILITY_TYPE,
        t.FACILITY_ID               = s.FACILITY_ID,
        t.FACILITYOI                = s.FACILITYOI,
        t.PIP_TYPE                  = s.PIP_TYPE,
        t.SHAPE                     = s.SHAPE,
        t.ACCESS_TYPE               = s.ACCESS_TYPE,
        t.MH_USE                    = s.MH_USE,
        t.WALL_MATERIAL             = s.WALL_MATERIAL,
        t.BENCH_MATERIAL            = s.BENCH_MATERIAL,
        t.CHANNEL_MATERIAL          = s.CHANNEL_MATERIAL,
        t.WALL_BYSIZE               = s.WALL_BYSIZE,
        t.WALL_DEPTH                = s.WALL_DEPTH,
        t.ELEVATION                 = s.ELEVATION,
        t.FRAME_MATERIAL            = s.FRAME_MATERIAL,
        t.HEIGHT                    = s.HEIGHT,
        t.UP_ELEVATION              = s.UP_ELEVATION,
        t.UP_GRADE_TO_INVERT        = s.UP_GRADE_TO_INVERT,
        t.DOWN_ELEVATION            = s.DOWN_ELEVATION,
        t.DOWN_GRADE_TO_INVERT      = s.DOWN_GRADE_TO_INVERT,
        t.STREET                    = s.STREET,
        t.TOTAL_LENGTH              = s.TOTAL_LENGTH,
        t.YEAR_CONSTRUCTED          = s.YEAR_CONSTRUCTED,
        t."SIZE"                    = s.SIZE_COL,
        t.DRAINAGE_AREA             = s.DRAINAGE_AREA,
        t.UNKNOWN_TYPE              = s.UNKNOWN_TYPE,
        t.CREATEDATE_DTTM           = s.CREATEDATE_DTTM,
        t.LASTUPDATE_DTTM           = s.LASTUPDATE_DTTM

      WHEN NOT MATCHED THEN INSERT (
        PROJECT_SID, INSPECTION_TYPE, INSPECTIONID, WORKORDER, WORKORDER_UUID, PROJECT, PO_NUMBER,
        ADDITIONAL_INFORMATION, PIPE_SEGMENT_REFERENCE, LATERAL_SEGMENT_REFERENCE,
        MANHOLE_NUMBER, MATERIAL, PIPE_USE, COVER_SHAPE, UPSTREAM_MH, DOWNSTREAM_MH,
        FEED_STATUS,
        FACILITY_TYPE, FACILITY_ID, FACILITYOI, PIP_TYPE, SHAPE, ACCESS_TYPE, MH_USE,
        WALL_MATERIAL, BENCH_MATERIAL, CHANNEL_MATERIAL, WALL_BYSIZE, WALL_DEPTH,
        ELEVATION, FRAME_MATERIAL,
        HEIGHT, UP_ELEVATION, UP_GRADE_TO_INVERT, DOWN_ELEVATION, DOWN_GRADE_TO_INVERT,
        STREET, TOTAL_LENGTH, YEAR_CONSTRUCTED, "SIZE", DRAINAGE_AREA,
        UNKNOWN_TYPE, CREATEDATE_DTTM, LASTUPDATE_DTTM
      ) VALUES (
        s.PROJECT_SID, s.INSPECTION_TYPE, s.INSPECTIONID, s.WORKORDER, s.WORKORDER_UUID, s.PROJECT, s.PO_NUMBER,
        s.CLEAN_INFO, s.PIPE_SEGMENT_REFERENCE, s.LATERAL_SEGMENT_REFERENCE,
        s.MANHOLE_NUMBER, s.MATERIAL, s.PIPE_USE, s.COVER_SHAPE, s.UPSTREAM_MH, s.DOWNSTREAM_MH,
        'NEW',
        s.FACILITY_TYPE, s.FACILITY_ID, s.FACILITYOI, s.PIP_TYPE, s.SHAPE, s.ACCESS_TYPE, s.MH_USE,
        s.WALL_MATERIAL, s.BENCH_MATERIAL, s.CHANNEL_MATERIAL, s.WALL_BYSIZE, s.WALL_DEPTH,
        s.ELEVATION, s.FRAME_MATERIAL,
        s.HEIGHT, s.UP_ELEVATION, s.UP_GRADE_TO_INVERT, s.DOWN_ELEVATION, s.DOWN_GRADE_TO_INVERT,
        s.STREET, s.TOTAL_LENGTH, s.YEAR_CONSTRUCTED, s.SIZE_COL, s.DRAINAGE_AREA,
        s.UNKNOWN_TYPE, s.CREATEDATE_DTTM, s.LASTUPDATE_DTTM
      );

    END LOOP;
  END AFTER STATEMENT;

END TRG_CR_INSPECT;