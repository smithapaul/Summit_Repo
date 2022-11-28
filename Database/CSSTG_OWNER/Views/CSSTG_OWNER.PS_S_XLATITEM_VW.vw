DROP VIEW CSSTG_OWNER.PS_S_XLATITEM_VW
/

--
-- PS_S_XLATITEM_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.PS_S_XLATITEM_VW
BEQUEATH DEFINER
AS 
SELECT a.fieldname,
          a.fieldvalue,
          a.effdt,
          a.src_sys_id,
          a.eff_status,
          a.xlatlongname,
          a.xlatshortname,
          a.lastupddttm,
          a.lastupdoprid,
          a.syncid,
          a.TIMEZONE,
          a.load_error,
          a.data_origin,
          a.created_ew_dttm,
          a.lastupd_ew_dttm,
          a.batch_sid
     FROM CSSTG_OWNER.PSXLATITEM a
    WHERE effdt =
             (SELECT MAX (a1.effdt)
                FROM CSSTG_OWNER.PSXLATITEM a1
               WHERE     a.fieldname = a1.fieldname
                     AND a.fieldvalue = a1.fieldvalue
                     AND a.src_sys_id = a1.src_sys_id
                     AND a.effdt <=
                            TO_DATE (TO_CHAR (SYSDATE, 'YYYY-MM-DD'),
                                     'YYYY-MM-DD'))
/
