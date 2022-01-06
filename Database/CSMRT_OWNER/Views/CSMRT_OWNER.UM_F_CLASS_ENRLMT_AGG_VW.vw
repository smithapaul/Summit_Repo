CREATE OR REPLACE VIEW UM_F_CLASS_ENRLMT_AGG_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           TERM_SID,
           SRC_SYS_ID,
           ENROLL_ORDER,
           ENROLL_DT,
           TO_NUMBER (TO_CHAR (ENROLL_DT, 'YYYYMMDD'))     ENROLL_DT_SID,
           ENROLL_CNT,
           ENROLL_CLASS_CNT,
           TAKEN_UNIT_SUM
      FROM (  SELECT /*+ PARALLEL(F,8) */
                     F.PERSON_SID,
                     F.TERM_SID,
                     F.SRC_SYS_ID,
                     MIN (0)
                         ENROLL_ORDER,
                     MIN (
                         CASE
                             WHEN     F.ENRL_ADD_DT > F.ENRL_DROP_DT
                                  AND TO_CHAR (F.ENRL_DROP_DT, 'YYYYMMDD') >
                                      '19500101'
                             THEN
                                 F.ENRL_DROP_DT
                             ELSE
                                 F.ENRL_ADD_DT
                         END)
                         ENROLL_DT,
                     (CASE
                          WHEN    MAX (TO_CHAR (F.ENRL_DROP_DT, 'YYYYMMDD')) >
                                  '19500101'
                               OR MAX (F.ENRL_DROP_DT) > MIN (F.ENRL_ADD_DT)
                               OR SUM (
                                      CASE
                                          WHEN S.ENRLMT_STAT_ID = 'E'
                                          THEN
                                              F.TAKEN_UNIT
                                          ELSE
                                              0
                                      END) >
                                  0
                          THEN
                              1
                          ELSE
                              0
                      END)
                         ENROLL_CNT,
                     SUM (CASE WHEN S.ENRLMT_STAT_ID = 'E' THEN 1 ELSE 0 END)
                         ENROLL_CLASS_CNT,
                     SUM (
                         CASE
                             WHEN S.ENRLMT_STAT_ID = 'E' THEN F.TAKEN_UNIT
                             ELSE 0
                         END)
                         TAKEN_UNIT_SUM
                FROM UM_F_CLASS_ENRLMT F, PS_D_ENRLMT_STAT S
               WHERE F.ENRLMT_STAT_SID = S.ENRLMT_STAT_SID
            GROUP BY F.PERSON_SID, F.TERM_SID, F.SRC_SYS_ID
            UNION ALL
              SELECT /*+ PARALLEL(F,8) */
                     F.PERSON_SID,
                     F.TERM_SID,
                     F.SRC_SYS_ID,
                     MIN (1)
                         ENROLL_ORDER,
                     MAX (F.ENRL_DROP_DT)
                         ENROLL_DT,
                     MIN (0)
                         ENROLL_CNT,
                     SUM (CASE WHEN S.ENRLMT_STAT_ID = 'E' THEN 1 ELSE 0 END)
                         ENROLL_CLASS_CNT,
                     SUM (
                         CASE
                             WHEN S.ENRLMT_STAT_ID = 'E' THEN F.TAKEN_UNIT
                             ELSE 0
                         END)
                         TAKEN_UNIT_SUM
                FROM UM_F_CLASS_ENRLMT F, PS_D_ENRLMT_STAT S
               WHERE F.ENRLMT_STAT_SID = S.ENRLMT_STAT_SID
            GROUP BY F.PERSON_SID, F.TERM_SID, F.SRC_SYS_ID
              HAVING     (    MAX (TO_CHAR (F.ENRL_DROP_DT, 'YYYYMMDD')) >
                              19500101
                          AND MAX (F.ENRL_DROP_DT) >= MIN (F.ENRL_ADD_DT))
                     AND SUM (
                             CASE
                                 WHEN S.ENRLMT_STAT_ID = 'E'
                                 THEN
                                     F.TAKEN_UNIT
                                 ELSE
                                     0
                             END) <=
                         0);
