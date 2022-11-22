DROP VIEW CSMRT_OWNER.UM_D_CLASS_MTG_PAT_AGG_VW
/

--
-- UM_D_CLASS_MTG_PAT_AGG_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_CLASS_MTG_PAT_AGG_VW
BEQUEATH DEFINER
AS 
WITH
        MTG
        AS
            (  SELECT CLASS_SID,
                      MIN (
                          DECODE (CLASS_MTG_PAT_ORDER,
                                  1, CLASS_MTG_PAT_SID,
                                  2147483646))
                          CLASS_MTG_PAT_SID1,
                      MIN (
                          DECODE (CLASS_MTG_PAT_ORDER,
                                  2, CLASS_MTG_PAT_SID,
                                  2147483646))
                          CLASS_MTG_PAT_SID2
                 FROM UM_D_CLASS_MTG_PAT
                WHERE DATA_ORIGIN <> 'D'
             GROUP BY CLASS_SID)
    SELECT MTG.CLASS_SID,
           M1.CLASS_MTG_NUM                   CLASS_MTG_NUM1,
           NVL (M1.FCLTY_SID, 2147483646)     FCLTY_SID1,
           M1.START_DT                        START_DT1,
           M1.END_DT                          END_DT1,
           M1.MTG_PAT_CD                      MTG_PAT_CD1,
           nvl(M1.MEETING_TIME,'-')           MEETING_TIME1,
           M2.CLASS_MTG_NUM                   CLASS_MTG_NUM2,
           NVL (M2.FCLTY_SID, 2147483646)     FCLTY_SID2,
           M2.START_DT                        START_DT2,
           M2.END_DT                          END_DT2,
           M2.MTG_PAT_CD                      MTG_PAT_CD2,
           nvl(M2.MEETING_TIME,'-')           MEETING_TIME2
      FROM MTG
           JOIN UM_D_CLASS_MTG_PAT M1
               ON MTG.CLASS_MTG_PAT_SID1 = M1.CLASS_MTG_PAT_SID
           left outer JOIN UM_D_CLASS_MTG_PAT M2
               ON MTG.CLASS_MTG_PAT_SID2 = M2.CLASS_MTG_PAT_SID
/
