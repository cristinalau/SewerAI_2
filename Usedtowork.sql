-- =====================================================================
-- Trigger: CUSTOMERDATA.TRG_CR_INSPECT
-- Purpose: For each new DR work-history row (EPDRFACWORKHISTORY),
--          insert exactly ONE row in EPSEWERAI_CR_INSPECT **per DR UUID**
--          (i.e., one row per inspection). No duplicates for the same DR.
--
-- Behavior (Option A):
--   • One row per DR (e.uuid). Multiple DRs for the same task will create
--     multiple CR rows (each with different inspectionid).
--   • The inserted row is created ONLY for the exact DR row being inserted,
--     guarded by e.uuid = :NEW.uuid.
--   • Dedupe guard ensures (project_sid, inspectionid) is not already present.
--
-- Notes:
--   • Includes defensive guards (WASS app id dash handling, prefix stripping).
--   • Leaves your site filter (wo.site_oi = 58) intact.
--   • Expects EPSEWERAI_CR_INSPECT.inspectionid and project_sid are RAW(16).
-- =====================================================================
CREATE OR REPLACE TRIGGER TRG_CR_INSPECT
FOR INSERT ON CUSTOMERDATA.EPDRFACWORKHISTORY
COMPOUND TRIGGER

  ------------------------------------------------------------------------
  -- Row buffer for inserted keys (one entry for each inserted DR row)
  ------------------------------------------------------------------------
  TYPE t_key IS RECORD (
    wotask_oi        NUMBER,
    epdrfacility_oi  NUMBER,
    dr_uuid          VARCHAR2(50)     -- DR UUID as text from :NEW.uuid
  );
  TYPE t_key_tab IS TABLE OF t_key;
  g_rows t_key_tab;

  ------------------------------------------------------------------------
  -- Initialize buffer
  ------------------------------------------------------------------------
  BEFORE STATEMENT IS
  BEGIN
    g_rows := t_key_tab();
  END BEFORE STATEMENT;

  ------------------------------------------------------------------------
  -- Buffer minimal keys for each inserted row
  ------------------------------------------------------------------------
  BEFORE EACH ROW IS
  BEGIN
    g_rows.EXTEND;
    g_rows(g_rows.COUNT).wotask_oi       := :NEW.wotask_oi;
    g_rows(g_rows.COUNT).epdrfacility_oi := :NEW.epdrfacility_oi;
    g_rows(g_rows.COUNT).dr_uuid         := :NEW.uuid;
  END BEFORE EACH ROW;

  ------------------------------------------------------------------------
  -- Insert one CR row per buffered DR row
  ------------------------------------------------------------------------
  AFTER STATEMENT IS
  BEGIN
    FOR i IN 1 .. g_rows.COUNT LOOP

      INSERT INTO CUSTOMERDATA.EPSEWERAI_CR_INSPECT (
        project_sid,                -- RAW(16) from a.uuid (task UUID)
        inspection_type,            -- 'PACP' | 'MACP' | 'LACP'
        inspectionid,               -- RAW(16) from e.uuid (DR UUID)
        workorder,                  -- wonumber||'.'||tasknumber (<=15)
        project,                    -- wotasktitle (<=503)
        po_number,                  -- assetnumber (<=50)
        additional_information,     -- longdescript
        pipe_segment_reference,     -- (<=15) per rules
        lateral_segment_reference,  -- (<=15) for LACP only
        manhole_number,             -- MH/CB stripped when applicable
        material,                   -- mapped code (SC: esc.pipetype -> code; else ep1.material -> code)
        Pipe_Use,                -- ? now Pioneer code (mapped from wwtype)
        cover_shape,                -- mapped via EPSEWERAI_SHAPE_CODE, default 'Z'
        upstream_mh,                -- ep1.usfacilityid (<=15)
        downstream_mh,              -- ep1.dsfacilityid (<=15)
        feed_status                 -- 'NEW'
      )
      SELECT
        /* project_sid (task UUID -> RAW(16)) */
        HEXTORAW(REPLACE(a.uuid, '-', '')),

        /* inspection_type */
        CASE
          WHEN e1.facilitytype IN (1, 2) THEN 'MACP'
          WHEN e1.facilitytype = 10 AND ep2.unknfacType IN (2, 3) THEN 'MACP'
          WHEN e1.facilitytype = 8 THEN 'PACP'
          WHEN e1.facilitytype = 10 AND ep2.unknfacType IN (4, 5, 6, 7, 8) THEN 'PACP'
          WHEN e1.facilitytype = 4 THEN 'LACP'
          ELSE NULL
        END,

        /* inspectionid (DR UUID -> RAW(16)) */
        HEXTORAW(REPLACE(e.uuid, '-', '')),

        /* workorder (<=15) */
        SUBSTR(wo.wonumber || '.' || a.tasknumber, 1, 15),

        /* project (<=503) */
        SUBSTR(a.wotasktitle, 1, 503),

        /* po_number (<=50) */
        SUBSTR(s.assetnumber, 1, 50),

        /* additional_information */
        a.longdescript,

        /* pipe_segment_reference (<=15) - NULL for MH/CB/LACP & unknown mapped to MH/CB */
        CASE
          WHEN e1.facilitytype IN (1, 2, 4) THEN NULL
          WHEN e1.facilitytype = 10 AND ep2.unknfacType IN (2, 3) THEN NULL
          ELSE SUBSTR(
                 CASE
                   WHEN e1.facilitytype = 10
                     THEN REGEXP_REPLACE(UPPER(ep2.unknfacid), '^U', '')
                   WHEN esc.wass_appid IS NOT NULL AND INSTR(esc.wass_appid, '-', -1) > 0
                     THEN SUBSTR(esc.wass_appid, 1, INSTR(esc.wass_appid, '-', -1) - 1)
                   WHEN ep1.pipeid IS NULL
                     THEN NULL
                   WHEN UPPER(ep1.pipeid) LIKE 'PIP%' THEN SUBSTR(ep1.pipeid, 4)
                   WHEN UPPER(ep1.pipeid) LIKE 'CBL%' THEN SUBSTR(ep1.pipeid, 4)
                   ELSE ep1.pipeid
                 END, 1, 15)
        END,

        /* lateral_segment_reference (only for LACP / facilitytype=4) */
        CASE
          WHEN e1.facilitytype = 4 THEN
            SUBSTR(
              CASE
                WHEN e1.facilitytype = 10
                  THEN REGEXP_REPLACE(UPPER(ep2.unknfacid), '^U', '')
                WHEN esc.wass_appid IS NOT NULL AND INSTR(esc.wass_appid, '-', -1) > 0
                  THEN SUBSTR(esc.wass_appid, 1, INSTR(esc.wass_appid, '-', -1) - 1)
                WHEN ep1.pipeid IS NULL
                  THEN NULL
                WHEN UPPER(ep1.pipeid) LIKE 'PIP%' THEN SUBSTR(ep1.pipeid, 4)
                WHEN UPPER(ep1.pipeid) LIKE 'CBL%' THEN SUBSTR(ep1.pipeid, 4)
                ELSE ep1.pipeid
              END, 1, 15)
          ELSE NULL
        END,

        /* manhole_number (strip MH/CB) */
        CASE
          WHEN e1.facilitytype = 1 THEN REGEXP_REPLACE(UPPER(e1.facilityid), '^MH', '')
          WHEN e1.facilitytype = 2 THEN REGEXP_REPLACE(UPPER(e1.facilityid), '^CB', '')
          WHEN e1.facilitytype = 10 AND ep2.unknfacType = 3 THEN REGEXP_REPLACE(UPPER(e1.facilityid), '^MH', '')
          WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN REGEXP_REPLACE(UPPER(e1.facilityid), '^CB', '')
          ELSE NULL
        END,

        /* material:
           - If Service Connection (type=4): map esc.pipetype (mx_sc), fallback -> mapped ep1.material (mx), fallback -> raw ep1.material
           - Else: mapped ep1.material (mx) -> raw ep1.material
        */
        CASE
          WHEN e1.facilitytype = 4 THEN
            COALESCE(mx_sc.pioneers_code, mx.pioneers_code, ep1.material)
          ELSE
            NVL(mx.pioneers_code, ep1.material)
        END,

        /* Pipe_Use -> Pioneer code (from wwtype) */
        CASE
          WHEN UPPER(TRIM(
                 CASE
                   WHEN e1.facilitytype = 2  THEN cb.wwtype
                   WHEN e1.facilitytype = 8  THEN ep1.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN cb.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 4 THEN ep1.wwtype
                   ELSE NULL
                 END
               )) = 'COMBINED'          THEN 'CB'
          WHEN UPPER(TRIM(
                 CASE
                   WHEN e1.facilitytype = 2  THEN cb.wwtype
                   WHEN e1.facilitytype = 8  THEN ep1.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN cb.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 4 THEN ep1.wwtype
                   ELSE NULL
                 END
               )) = 'FOUNDATION DRAIN'  THEN 'PN'
          WHEN UPPER(TRIM(
                 CASE
                   WHEN e1.facilitytype = 2  THEN cb.wwtype
                   WHEN e1.facilitytype = 8  THEN ep1.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN cb.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 4 THEN ep1.wwtype
                   ELSE NULL
                 END
               )) = 'SANITARY'          THEN 'SS'
          WHEN UPPER(TRIM(
                 CASE
                   WHEN e1.facilitytype = 2  THEN cb.wwtype
                   WHEN e1.facilitytype = 8  THEN ep1.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN cb.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 4 THEN ep1.wwtype
                   ELSE NULL
                 END
               )) = 'STORM'             THEN 'SW'
          WHEN UPPER(TRIM(
                 CASE
                   WHEN e1.facilitytype = 2  THEN cb.wwtype
                   WHEN e1.facilitytype = 8  THEN ep1.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN cb.wwtype
                   WHEN e1.facilitytype = 10 AND ep2.unknfacType = 4 THEN ep1.wwtype
                   ELSE NULL
                 END
               )) IN ('WATER', 'NOT APPLICABLE', 'N/A', 'NA') THEN 'XX'
          ELSE NULL
        END,

        /* cover_shape via lookup (default 'Z') */
        COALESCE(sc.pioneers_code, 'Z') AS cover_shape,

        /* upstream/downstream (<=15) */
        SUBSTR(ep1.usfacilityid, 1, 15),
        SUBSTR(ep1.dsfacilityid, 1, 15),

        /* feed_status */
        'NEW'
      FROM mnt.workordertask                    a
      JOIN customerdata.epdrfacworkhistory      e   ON e.wotask_oi = a.workordertaskoi
      LEFT JOIN mnt.workorders                  wo  ON a.workorder_oi = wo.workordersoi
      LEFT JOIN mnt.asset                       s   ON a.asset_oi = s.assetoi
      LEFT JOIN customerdata.epdrdrainfacility  e1  ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi
      LEFT JOIN customerdata.epdrpipe           ep1 ON e1.epdrpipe_oi = ep1.epdrpipeoi
      LEFT JOIN customerdata.epdrunknfac        ep2 ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi
      LEFT JOIN customerdata."EPDRSERVICECONNECT" esc ON esc.wass_appid = e1.facilityid
      LEFT JOIN customerdata.epdrcatchbasin     cb  ON cb.catchbasinid = e1.facilityid

      /* Mapping joins */
      LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mx
        ON UPPER(TRIM(mx.ivara_material)) = UPPER(TRIM(ep1.material))
      LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mx_sc
        ON UPPER(TRIM(mx_sc.ivara_material)) = UPPER(TRIM(esc.pipetype))

      /* Shape lookup join — uses ep1.shape for pipes, otherwise cb.shape */
      LEFT JOIN CUSTOMERDATA.EPSEWERAI_SHAPE_CODE sc
        ON UPPER(TRIM(sc.ivara_shape)) =
           UPPER(TRIM(
             CASE
               WHEN e1.facilitytype = 8 THEN ep1.shape
               ELSE cb.shape
             END
           ))

      WHERE wo.site_oi        = 58
        AND e.wotask_oi       = g_rows(i).wotask_oi
        AND e.epdrfacility_oi = g_rows(i).epdrfacility_oi
        -- Only the just-inserted DR row
        AND e.uuid            = g_rows(i).dr_uuid
        -- De-dupe: prevent same (project_sid, inspectionid) from being inserted twice
        AND NOT EXISTS (
              SELECT 1
              FROM CUSTOMERDATA.EPSEWERAI_CR_INSPECT t
              WHERE t.project_sid  = HEXTORAW(REPLACE(a.uuid, '-', ''))
                AND t.inspectionid = HEXTORAW(REPLACE(e.uuid, '-', ''))
            );

    END LOOP;
  END AFTER STATEMENT;

END TRG_CR_INSPECT;
/

ALTER TRIGGER TRG_CR_INSPECT ENABLE;
