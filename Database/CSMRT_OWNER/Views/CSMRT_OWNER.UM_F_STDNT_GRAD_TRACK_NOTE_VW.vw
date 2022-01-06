CREATE OR REPLACE VIEW UM_F_STDNT_GRAD_TRACK_NOTE_VW
BEQUEATH DEFINER
AS 
SELECT DISTINCT G.PERSON_ID,
                    G.INSTITUTION_CD,
                    G.ACAD_CAR_CD,
                    G.STDNT_CAR_NUM,
                    G.ACAD_PROG_CD,
                    G.EXP_GRAD_TERM,
                    G.DEG_CD,
                    NVL (N.SEQNUM, 1)                             SEQNUM,
                    G.SRC_SYS_ID,
                    DENSE_RANK ()
                        OVER (PARTITION BY G.PERSON_ID,
                                           G.INSTITUTION_CD,
                                           G.ACAD_CAR_CD,
                                           G.STDNT_CAR_NUM,
                                           G.ACAD_PROG_CD,
                                           G.EXP_GRAD_TERM,
                                           G.DEG_CD,
                                           G.SRC_SYS_ID
                              ORDER BY NVL (N.SEQNUM, 1) DESC)    NOTE_ORDER,
                    N.SSR_GRAD_NOTE,
                    N.DESCR,
                    N.SCC_ROW_ADD_OPRID,
                    N.SCC_ROW_ADD_DTTM,
                    N.SSR_GRAD_NOTE_LONG,
                    N.DATA_ORIGIN,
                    N.CREATED_EW_DTTM,
                    N.LASTUPD_EW_DTTM
      FROM CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK  G
           LEFT OUTER JOIN CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE N
               ON     G.PERSON_ID = N.PERSON_ID
                  AND G.INSTITUTION_CD = N.INSTITUTION_CD
                  AND G.ACAD_CAR_CD = N.ACAD_CAR_CD
                  AND G.STDNT_CAR_NUM = N.STDNT_CAR_NUM
                  AND G.ACAD_PROG_CD = N.ACAD_PROG_CD
                  AND G.EXP_GRAD_TERM = N.EXP_GRAD_TERM
                  AND G.DEG_CD = N.DEG_CD
                  AND G.SRC_SYS_ID = N.SRC_SYS_ID;
