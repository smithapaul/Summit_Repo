DROP VIEW CSMRT_OWNER.UM_R_STDNT_GRP_VW2
/

--
-- UM_R_STDNT_GRP_VW2  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_R_STDNT_GRP_VW2
BEQUEATH DEFINER
AS 
SELECT /*+ use_hash(G,T) */
           G.TERM_SID,
           G.PERSON_SID,
           G.STDNT_GRP_SID,
           G.SRC_SYS_ID,
           T.INSTITUTION_SID,
           G.INSTITUTION_CD,
           G.ACAD_CAR_CD,
           G.TERM_CD,
           G.PERSON_ID,
           G.STDNT_GRP_CD,
           G.EFFDT,
           G.EFF_STAT_CD,
           G.EFF_START_DT     TERM_BEGIN_DT,
           G.EFF_END_DT       TERM_END_DT,
           G.HONORS_FLG,
           G.STDNT_GRP_ORDER,
           G.COMMENTS
      FROM UM_R_STDNT_GRP G JOIN PS_D_TERM T ON G.TERM_SID = T.TERM_SID
     where ROWNUM < 100000000
/
