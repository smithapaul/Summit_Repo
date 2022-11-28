DROP VIEW CSSTG_OWNER.UM_TERM_TBL_OLD_VW
/

--
-- UM_TERM_TBL_OLD_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.UM_TERM_TBL_OLD_VW
BEQUEATH DEFINER
AS 
WITH T1
        AS (SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   TERM_END_DT,
                   NVL (
                      MIN (
                         TRUNC (TERM_BEGIN_DT))
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                      TO_DATE ('31-DEC-9999'))
                      NEXT_BEGIN_DT,
                   NVL (
                      MAX (
                         TRUNC (TERM_BEGIN_DT))
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      TO_DATE ('01-JAN-1900'))
                      PREV_BEGIN_DT,
                   NVL (
                      MAX (
                         CASE
                            WHEN (   SUBSTR (STRM, -1, 1) IN ('2', '6')
                                  OR SUBSTR (STRM, -2, 2) IN ('10', '30'))
                            THEN
                               TRUNC (TERM_BEGIN_DT)
                            ELSE
                               TO_DATE ('01-JAN-1900')
                         END)
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      TO_DATE ('01-JAN-1900'))
                      PREV_FULL_TERM_BEGIN_DT
              FROM CSSTG_OWNER.PS_TERM_TBL
             WHERE     DATA_ORIGIN <> 'D'
                   AND STRM <= '9000'
                   --  and substr(STRM,3,2) not in ('50','90')
                   AND SUBSTR (STRM, 3, 2) NOT IN ('50')
                   AND SUBSTR (STRM, -1, 1) NOT IN ('5', '7', '9')),
        T2
        AS (SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   (CASE
                       WHEN TERM_END_DT >= NEXT_BEGIN_DT
                       THEN
                          NEXT_BEGIN_DT - 1
                       ELSE
                          TERM_END_DT
                    END)
                      TERM_END_DT,
                   PREV_BEGIN_DT,
                   PREV_FULL_TERM_BEGIN_DT
              FROM T1),
        T3
        AS (SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   TERM_END_DT,
                   LEAST (
                      NVL (
                         MIN (
                            TRUNC (TERM_END_DT) + 1)
                         OVER (
                            PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                         TO_DATE ('01-JAN-1900')),
                      TERM_BEGIN_DT)
                      EARLY_BEGIN_DT,
                   NVL (
                      MIN (
                         TRUNC (TERM_BEGIN_DT) - 1)
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                      TO_DATE ('31-DEC-9999'))
                      LATE_END_DT,
                   PREV_BEGIN_DT,
                   PREV_FULL_TERM_BEGIN_DT
              FROM T2),
        T4
        AS (SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   TERM_END_DT,
                   EARLY_BEGIN_DT,
                   LATE_END_DT,
                   PREV_BEGIN_DT,
                   PREV_FULL_TERM_BEGIN_DT
              FROM T3
            UNION
            SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   TERM_END_DT,
                   EARLY_BEGIN_DT,
                   LATE_END_DT,
                   PREV_BEGIN_DT,
                   PREV_FULL_TERM_BEGIN_DT
              FROM (SELECT INSTITUTION,
                           ACAD_CAREER,
                           STRM,
                           SRC_SYS_ID,
                           TERM_BEGIN_DT,
                           TERM_END_DT,
                           LEAST (
                              NVL (
                                 MIN (
                                    TRUNC (TERM_END_DT) + 1)
                                 OVER (
                                    PARTITION BY INSTITUTION,
                                                 ACAD_CAREER,
                                                 SRC_SYS_ID
                                    ORDER BY STRM
                                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                                 TO_DATE ('01-JAN-1900')),
                              TERM_BEGIN_DT)
                              EARLY_BEGIN_DT,
                           NVL (
                              MIN (
                                 TRUNC (TERM_BEGIN_DT) - 1)
                              OVER (
                                 PARTITION BY INSTITUTION,
                                              ACAD_CAREER,
                                              SRC_SYS_ID
                                 ORDER BY STRM
                                 ROWS BETWEEN 1 FOLLOWING
                                      AND     UNBOUNDED FOLLOWING),
                              TO_DATE ('31-DEC-9999'))
                              LATE_END_DT,
                           NVL (
                              MAX (
                                 TRUNC (TERM_BEGIN_DT))
                              OVER (
                                 PARTITION BY INSTITUTION,
                                              ACAD_CAREER,
                                              SRC_SYS_ID
                                 ORDER BY STRM
                                 ROWS BETWEEN UNBOUNDED PRECEDING
                                      AND     1 PRECEDING),
                              TO_DATE ('01-JAN-1900'))
                              PREV_BEGIN_DT,
                           NVL (
                              MAX (
                                 CASE
                                    WHEN (   SUBSTR (STRM, -1, 1) IN ('2',
                                                                      '6')
                                          OR SUBSTR (STRM, -2, 2) IN ('10',
                                                                      '30'))
                                    THEN
                                       TRUNC (TERM_BEGIN_DT)
                                    ELSE
                                       TO_DATE ('01-JAN-1900')
                                 END)
                              OVER (
                                 PARTITION BY INSTITUTION,
                                              ACAD_CAREER,
                                              SRC_SYS_ID
                                 ORDER BY STRM
                                 ROWS BETWEEN UNBOUNDED PRECEDING
                                      AND     1 PRECEDING),
                              TO_DATE ('01-JAN-1900'))
                              PREV_FULL_TERM_BEGIN_DT
                      FROM CSSTG_OWNER.PS_TERM_TBL
                     WHERE     DATA_ORIGIN <> 'D'
                           AND STRM <= '9000'
                           --  and substr(STRM,3,2) not in ('40','90')
                           AND SUBSTR (STRM, 3, 2) NOT IN ('40')
                           AND SUBSTR (STRM, -1, 1) NOT IN ('4', '7', '9'))
             WHERE (   SUBSTR (STRM, 3, 2) IN ('50')
                    OR SUBSTR (STRM, -1, 1) = '5')
            UNION
            SELECT INSTITUTION,
                   ACAD_CAREER,
                   STRM,
                   SRC_SYS_ID,
                   TERM_BEGIN_DT,
                   TERM_END_DT,
                   TRUNC (TERM_BEGIN_DT) EARLY_BEGIN_DT,
                   TRUNC (TERM_END_DT) LATE_END_DT,
                   NVL (
                      MAX (
                         TRUNC (TERM_BEGIN_DT))
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      TO_DATE ('01-JAN-1900'))
                      PREV_BEGIN_DT,
                   NVL (
                      MAX (
                         CASE
                            WHEN (   SUBSTR (STRM, -1, 1) IN ('2', '6')
                                  OR SUBSTR (STRM, -2, 2) IN ('10', '30'))
                            THEN
                               TRUNC (TERM_BEGIN_DT)
                            ELSE
                               TO_DATE ('01-JAN-1900')
                         END)
                      OVER (PARTITION BY INSTITUTION, ACAD_CAREER, SRC_SYS_ID
                            ORDER BY STRM
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                      TO_DATE ('01-JAN-1900'))
                      PREV_FULL_TERM_BEGIN_DT
              FROM CSSTG_OWNER.PS_TERM_TBL
             WHERE     DATA_ORIGIN <> 'D' --  and (substr(STRM,3,2) in ('90') or substr(STRM,-1,1) in ('7','9') or STRM > '9000')
                   AND (SUBSTR (STRM, -1, 1) IN ('7', '9') OR STRM > '9000'))
   SELECT INSTITUTION,
          ACAD_CAREER,
          STRM,
          SRC_SYS_ID,
          TERM_BEGIN_DT,
          TERM_END_DT,
          EARLY_BEGIN_DT,
          LATE_END_DT,
          PREV_BEGIN_DT,
          PREV_FULL_TERM_BEGIN_DT,
          (CASE
              WHEN STRM >= '1010' AND SUBSTR (STRM, 3, 2) IN ('10', '30')
              THEN
                 'Y'
              WHEN STRM < '1010' AND SUBSTR (STRM, -1, 1) IN ('2', '6')
              THEN
                 'Y'
              ELSE
                 'N'
           END)
             FULL_TERM_FLAG
     FROM T4
/
