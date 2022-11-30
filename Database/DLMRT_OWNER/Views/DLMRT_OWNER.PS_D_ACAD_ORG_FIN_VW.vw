DROP VIEW DLMRT_OWNER.PS_D_ACAD_ORG_FIN_VW
/

--
-- PS_D_ACAD_ORG_FIN_VW  (View) 
--
CREATE OR REPLACE VIEW DLMRT_OWNER.PS_D_ACAD_ORG_FIN_VW
BEQUEATH DEFINER
AS 
WITH
        ACAD_ORGS
        AS
            (SELECT *
               FROM CSMRT_OWNER.PS_D_ACAD_ORG
              WHERE EFFDT_ORDER = 1 AND ACAD_ORG_CD <> '-'),
        ACAD_OWN
        AS
            (SELECT /*+PARALLEL(8) inline no_merge
                                               USE_HASH(A)
                                               */
                    ACAD_ORG_CD                               AS ACAD_ORG,
                    EFFDT,
                    INSTITUTION_CD                            AS BUSINESS_UNIT,
                    DEPTID,
                    SUBSTR (DEPTID || '000000000', 1, 10)     DEPTID_2, --temp to work around bad data
                    PERCENT_OWNED
               FROM CSMRT_OWNER.PS_R_ACAD_ORG_FS_OWN A
              WHERE EFFDT =
                    (SELECT MAX (EFFDT)
                       FROM CSMRT_OWNER.PS_R_ACAD_ORG_FS_OWN B
                      WHERE     A.ACAD_ORG_CD = B.ACAD_ORG_CD
                            AND A.INSTITUTION_CD = B.INSTITUTION_CD)),
        TREE
        AS
            (SELECT TREE.*, ORG.DEPT_DESCR
               FROM FSMRT_OWNER.ORGANIZATION_TREE_DIM  TREE,
                    FSMRT_OWNER.TREE_XREF_DIM          XREF,
                    FSMRT_OWNER.ORGANIZATION_DIM       ORG
              WHERE     TREE.SETID = XREF.SETID
                    AND TREE.TREE_XREF_KEY_ID = XREF.TREE_XREF_KEY_ID
                    AND ORG.ORG_KEY_ID = TREE.ORG_KEY_ID
                    AND TREE.CURRENT_FLAG = 'Y'
                    AND XREF.TREE_NAME = 'RPT_DEPARTMENT')
    SELECT /*+PARALLEL(8) inline
                                      USE_HASH(ACAD_ORGS ACAD_OWN TREE)
                                      */
           ACAD_OWN.BUSINESS_UNIT AS INSTITUTION_CD,
           ACAD_OWN.ACAD_ORG AS ACAD_ORG_CD,
           ACAD_ORGS.ACAD_ORG_LD,
           ACAD_ORGS.ACAD_ORG_SD,
           ACAD_ORGS.ACAD_ORG_FD,
           ACAD_OWN.PERCENT_OWNED,
           ACAD_OWN.DEPTID,
           ACAD_OWN.DEPTID_2,
           TREE.DEPT_DESCR,
           TREE.ORG_LEVEL_1,
           TREE.ORG_LEVEL_1_DESCR,
           TREE.ORG_LEVEL_2,
           TREE.ORG_LEVEL_2_DESCR,
           TREE.ORG_LEVEL_3,
           TREE.ORG_LEVEL_3_DESCR,
           TREE.ORG_LEVEL_4,
           TREE.ORG_LEVEL_4_DESCR,
           TREE.ORG_LEVEL_5,
           TREE.ORG_LEVEL_5_DESCR,
           TREE.ORG_LEVEL_6,
           TREE.ORG_LEVEL_6_DESCR
      FROM ACAD_OWN ACAD_OWN  
           LEFT OUTER JOIN ACAD_ORGS ACAD_ORGS
               ON ACAD_ORGS.ACAD_ORG_CD = ACAD_OWN.ACAD_ORG
           LEFT OUTER JOIN TREE
               ON     ACAD_OWN.DEPTID_2 = TREE.DEPTID
                  AND ACAD_OWN.BUSINESS_UNIT = TREE.SETID
/
