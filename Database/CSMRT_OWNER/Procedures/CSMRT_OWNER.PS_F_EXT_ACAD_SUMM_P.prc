CREATE OR REPLACE PROCEDURE             "PS_F_EXT_ACAD_SUMM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table PS_F_EXT_ACAD_SUMM.
--
 --V01  SMT-xxxx 07/13/2018,    James Doucette
--                              Converted from SQL Script
-- V01.2  SMT-8300 09/06/2017,    James Doucette
--                                Added two new fields.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_EXT_ACAD_SUMM';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_EXT_ACAD_SUMM');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM disable constraint PK_PS_F_EXT_ACAD_SUMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';				
insert /*+ append */ into CSMRT_OWNER.PS_F_EXT_ACAD_SUMM 
  with INST as (
select distinct INSTITUTION 
  from CSSTG_OWNER.PS_INSTITUTION_TBL 
 where DATA_ORIGIN <> 'D'),
       ALL_LIST as (
SELECT /*+ PARALLEL(8) INLINE */ distinct EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, E.SRC_SYS_ID, I.INSTITUTION
  FROM CSSTG_OWNER.PS_EXT_ACAD_SUM E, INST I
 WHERE E.DATA_ORIGIN <> 'D'
 UNION
SELECT /*+ PARALLEL(8) INLINE */ EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, E.SRC_SYS_ID, I.INSTITUTION
  FROM CSSTG_OWNER.PS_EXT_ACAD_DATA E, INST I
 WHERE E.DATA_ORIGIN <> 'D'
 UNION ALL
SELECT /*+ PARALLEL(8) INLINE */ EMPLID, EXT_ORG_ID, 'UN' EXT_CAREER, 0 EXT_DATA_NBR, E.SRC_SYS_ID, I.INSTITUTION
  FROM (select EMPLID, EXT_ORG_ID, SRC_SYS_ID from CSSTG_OWNER.PS_ACAD_HISTORY where DATA_ORIGIN <> 'D'
         minus
        select distinct EMPLID, EXT_ORG_ID, SRC_SYS_ID from CSSTG_OWNER.PS_EXT_ACAD_SUM where DATA_ORIGIN <> 'D'
         minus
        select distinct EMPLID, EXT_ORG_ID, SRC_SYS_ID from CSSTG_OWNER.PS_EXT_ACAD_DATA where DATA_ORIGIN <> 'D') E, INST I),
LVL_LIST AS (
SELECT DEG.DEGREE,
       DEG.SRC_SYS_ID,
       DEG.EFF_STATUS,
       DEG.EDUCATION_LVL,
       ROW_NUMBER() OVER (PARTITION BY DEG.DEGREE, DEG.SRC_SYS_ID 
                              ORDER BY DECODE(DEG.EFF_STATUS, 'A', 1, 99999), DEG.EFFDT DESC) ORD
  FROM CSSTG_OWNER.PS_DEGREE_TBL DEG
 WHERE DEG.DATA_ORIGIN <> 'D'),
       DEG_LIST AS (
SELECT /*+ PARALLEL(8) INLINE */ 
       DEG.EMPLID, DEG.EXT_ORG_ID, DEG.SRC_SYS_ID,
       DEG.EXT_DATA_NBR, D.EDUCATION_LVL,
       NVL ( TO_CHAR(DEG.DEGREE_DT, 'YYYYMM'), '190001') DEG_MONTH,
       ROW_NUMBER() OVER (PARTITION BY DEG.EMPLID, DEG.EXT_ORG_ID, DEG.SRC_SYS_ID 
                              ORDER BY TO_CHAR(DEG.DEGREE_DT, 'YYYYMM') DESC,
                                       DECODE (EDUCATION_LVL, '21', 1, '15', 2, '18', 3, '17', 4, '14', 5, '13', 6, '10', 7, 99999999)) ORD,
       ROW_NUMBER() OVER (PARTITION BY DEG.EMPLID, DEG.EXT_ORG_ID, DEG.SRC_SYS_ID 
                              ORDER BY DECODE (EDUCATION_LVL, '21', 1, '15', 2, '18', 3, '17', 4, '14', 5, '13', 6, '10', 7, 99999999),
                                       TO_CHAR(DEG.DEGREE_DT, 'YYYYMM') DESC) ORD_LVL                        
  FROM CSSTG_OWNER.PS_EXT_DEGREE DEG, LVL_LIST D 
 WHERE DEG.DATA_ORIGIN <> 'D'
   AND DEG.DEGREE_STATUS = 'C'
   AND DEG.DEGREE = D.DEGREE
   AND D.ORD = 1),
       EXT_LIST AS (
SELECT /*+ PARALLEL(8) INLINE */
       EXT_ORG_ID, SRC_SYS_ID, 
       LS_SCHOOL_TYPE,
       ROW_NUMBER() OVER (PARTITION BY EXT_ORG_ID, SRC_SYS_ID 
                              ORDER BY decode(EFF_STATUS,'A',0,9), EFFDT desc) ORD
  FROM CSSTG_OWNER.PS_EXT_ORG_TBL_ADM
 WHERE DATA_ORIGIN <> 'D'),
     S as (
SELECT /*+ PARALLEL(8) INLINE */ 
       A.INSTITUTION INSTITUTION_CD,
       A.EMPLID PERSON_ID,
       A.EXT_ORG_ID,
       A.EXT_CAREER EXT_ACAD_CAR_ID,
       A.EXT_DATA_NBR,
       NVL(S.EXT_SUMM_TYPE, '-') EXT_SUMM_TYPE_ID,
       A.SRC_SYS_ID,
       NVL(S.EXT_ACAD_LEVEL, '-') EXT_ACAD_LVL_ID,
       nvl(S.TERM_YEAR,0) TERM_YEAR,
       NVL(S.EXT_TERM_TYPE, '-') EXT_TERM_TYPE,
       NVL(S.EXT_TERM, '-') EXT_TERM_ID,
       NVL(S.UNT_TYPE, '-') ACAD_UNIT_TYPE_ID,
       NVL(S.UNT_ATMP_TOTAL, 0) UNITS_ATTMPTD,
       NVL(S.UNT_COMP_TOTAL, 0) UNITS_CMPLTD,
       NVL(S.CLASS_RANK, 0) CLASS_RANK,
       NVL(S.CLASS_SIZE, 0) CLASS_SIZE,
       NVL(S.GPA_TYPE, '-') GPA_TYPE_ID,
       NVL(S.EXT_GPA, 0) EXT_GPA,
       NVL(S.CONVERT_GPA, 0) CONVERTED_GPA,
       NVL(S.PERCENTILE, 0) CLASS_PERCENTILE,
       NVL(S.RANK_TYPE, '-') ACAD_RANK_TYPE_ID,
       NVL(D.LS_DATA_SOURCE, '-') LS_DATA_SOURCE, 
       NVL(D.TRANSCRIPT_FLAG, '-') TRNSCR_FLG, 
       NVL(D.TRANSCRIPT_TYPE, '-') TRNSCR_TYPE, 
       NVL(D.TRNSCRPT_STATUS, '-') TRNSCR_STATUS, 
       D.TRANSCRIPT_DT TRNSCR_DT, 
       D.FROM_DT,  
       D.TO_DT, 
       NVL(D.EXT_ACAD_LEVEL, '-') D_EXT_ACAD_LEVEL,
       NVL(D.TERM_YEAR, 0) D_TERM_YEAR,
       NVL(D.EXT_TERM_TYPE, '-') D_EXT_TERM_TYPE,
       NVL(D.EXT_TERM, '-') D_EXT_TERM,
       NVL(G.UM_CUM_CREDIT, 0) UM_CUM_CREDIT,
       NVL(G.UM_CUM_GPA, 0) UM_CUM_GPA,
       NVL(G.UM_CUM_QP, 0) UM_CUM_QP,
       NVL(S.UM_GPA_EXCLUDE, '-') UM_GPA_EXCLUDE_FLG,
       NVL(S.UM_EXT_ORG_CR, 0) UM_EXT_ORG_CR,
       NVL(S.UM_EXT_ORG_QP, 0) UM_EXT_ORG_QP,
       NVL(S.UM_EXT_ORG_GPA, 0) UM_EXT_ORG_GPA,
       NVL(S.UM_EXT_ORG_CNV_CR, 0) UM_EXT_ORG_CNV_CR,
       NVL(S.UM_EXT_ORG_CNV_GPA, 0) UM_EXT_ORG_CNV_GPA,
       NVL(S.UM_EXT_ORG_CNV_QP, 0) UM_EXT_ORG_CNV_QP,
       NVL(S.UM_GPA_OVERRIDE, '-') UM_GPA_OVRD_FLG,     -- May 2017 
       NVL(S.UM_1_OVR_HSGPA, '-') UM_1_OVRD_HSGPA_FLG,      -- May 2017 
	   
	   NVL(S.UM_EXT_OR_MTSC_GPA, 0) UM_EXT_OR_MTSC_GPA,     -- SMT-8300 Sep. 2019 
	   NVL(S.MS_CONVERT_GPA, 0) MS_CONVERT_GPA,             -- SMT-8300 Sep. 2019 
	   
       NVL(S.UM_CONVERT_GPA, 0) UM_CONVERT_GPA,
       CASE WHEN S.EXT_SUMM_TYPE = 'HSOV' OR EXT.LS_SCHOOL_TYPE = 'SCD' 
            THEN CASE WHEN (ROW_NUMBER() OVER (PARTITION BY A.EMPLID, A.INSTITUTION
                                                   ORDER BY DECODE(S.EXT_SUMM_TYPE, 'HSOV', 1, 99999999),
                                                            DECODE(EXT.LS_SCHOOL_TYPE, 'SCD', 1, 99999999),
                                                            NVL(DEG.DEG_MONTH, '190001') DESC,
                                                            TO_CHAR(D.TO_DT, 'YYYYMM') DESC,
                                                            TO_CHAR(D.FROM_DT, 'YYYYMM') DESC,
                                                            NVL(S.CONVERT_GPA, 0) DESC,
                                                            NVL(S.TERM_YEAR, 0) DESC,
                                                            NVL(A.EXT_ORG_ID, '-') DESC,
                                                            A.EXT_DATA_NBR desc,    -- Sep 2019  
                                                            A.EXT_CAREER            -- Sep 2019 
                                                            ) ) = 1
                      THEN 'Y' 
                      ELSE 'N' 
                  END 
            WHEN S.EXT_SUMM_TYPE = 'UGOV' OR S.EXT_SUMM_TYPE = 'PBOV' 
            THEN CASE WHEN (ROW_NUMBER () OVER (PARTITION BY A.EMPLID, A.INSTITUTION
                                                    ORDER BY DECODE(S.EXT_SUMM_TYPE, 'UGOV', 1, 'PBOV', 2, 99999999),
                                                             DECODE(NVL(DEG2.EDUCATION_LVL, '-'), '21', 1, '15', 2, '18', 3, '17', 4, '14', 5, '13', 6, '10', 7, 99999999),
                                                             NVL(DEG.DEG_MONTH, '190001') DESC,
                                                             TO_CHAR(D.TO_DT, 'YYYYMM') DESC,
                                                             TO_CHAR(D.FROM_DT, 'YYYYMM') DESC,
                                                             NVL(S.CONVERT_GPA, 0) DESC,
                                                             NVL(S.TERM_YEAR, 0) DESC,
                                                             NVL(A.EXT_ORG_ID, '-') DESC,
                                                             A.EXT_DATA_NBR desc,    -- Sep 2019  
                                                             A.EXT_CAREER            -- Sep 2019 
                                                             ) ) = 1 
                      THEN 'Y' 
                      ELSE 'N' 
                  END                 
             ELSE 'N'
        END BEST_SUMM_TYPE_GPA_FLG,
       'S' DATA_ORIGIN
  FROM ALL_LIST A
  LEFT OUTER JOIN CSSTG_OWNER.PS_EXT_ACAD_DATA D
          ON A.EMPLID = D.EMPLID
         AND A.EXT_ORG_ID = D.EXT_ORG_ID 
         AND A.EXT_CAREER = D.EXT_CAREER
         AND A.EXT_DATA_NBR = D.EXT_DATA_NBR
         AND A.SRC_SYS_ID = D.SRC_SYS_ID
         AND D.DATA_ORIGIN <> 'D'  
  LEFT OUTER JOIN CSSTG_OWNER.PS_EXT_ACAD_SUM S
          ON S.EMPLID = A.EMPLID
         AND S.EXT_ORG_ID = A.EXT_ORG_ID 
         AND S.EXT_CAREER = A.EXT_CAREER
         AND S.EXT_DATA_NBR = A.EXT_DATA_NBR
         AND S.SRC_SYS_ID = A.SRC_SYS_ID
         AND S.INSTITUTION = A.INSTITUTION
         AND S.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_UM_CUMGPA G
          ON A.EMPLID = G.EMPLID
         AND A.INSTITUTION = G.INSTITUTION
         AND S.EXT_SUMM_TYPE = G.EXT_SUMM_TYPE
         AND A.SRC_SYS_ID = G.SRC_SYS_ID
         AND G.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN DEG_LIST DEG
          ON A.EMPLID = DEG.EMPLID
         AND A.EXT_ORG_ID = DEG.EXT_ORG_ID
         AND A.SRC_SYS_ID = DEG.SRC_SYS_ID
         AND A.EXT_DATA_NBR = DEG.EXT_DATA_NBR
         AND DEG.ORD = 1
  LEFT OUTER JOIN DEG_LIST DEG2
          ON A.EMPLID = DEG2.EMPLID
         AND A.EXT_ORG_ID = DEG2.EXT_ORG_ID
         AND A.SRC_SYS_ID = DEG2.SRC_SYS_ID
         AND DEG2.ORD_LVL = 1                
  LEFT OUTER JOIN EXT_LIST EXT
          ON A.EXT_ORG_ID = EXT.EXT_ORG_ID
         AND A.SRC_SYS_ID = EXT.SRC_SYS_ID
         AND EXT.ORD = 1
 where length(trim(A.EMPLID)) = 8 
   and A.EMPLID between '00000000' and '99999999')
select /*+ parallel(16) */
       S.INSTITUTION_CD, 
       S.PERSON_ID, 
       S.EXT_ORG_ID, 
       S.EXT_ACAD_CAR_ID, 
       S.EXT_DATA_NBR, 
       S.EXT_SUMM_TYPE_ID, 
       S.SRC_SYS_ID, 
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(P.PERSON_SID, 2147483646) APPLCNT_SID, 
       nvl(O.EXT_ORG_SID, 2147483646) EXT_ORG_SID, 
       nvl(EC.EXT_ACAD_CAR_SID, 2147483646) EXT_ACAD_CAR_SID, 
       nvl(ES.EXT_SUMM_TYPE_SID, 2147483646) EXT_SUMM_TYPE_SID, 
       nvl(RT.ACAD_RANK_TYPE_SID, 2147483646) ACAD_RANK_TYPE_SID, 
       nvl(UT.ACAD_UNIT_TYPE_SID, 2147483646) ACAD_UNIT_TYPE_SID, 
       nvl(EL.EXT_ACAD_LVL_SID, 2147483646) EXT_ACAD_LVL_SID, 
       nvl(ET.EXT_TERM_SID, 2147483646) EXT_TERM_SID, 
       S.TERM_YEAR EXT_TERM_YEAR_SID, 
       nvl(GT.GPA_TYPE_SID, 2147483646) GPA_TYPE_SID, 
       nvl(DL.EXT_ACAD_LVL_SID, 2147483646) D_EXT_ACAD_LVL_SID, 
       S.D_TERM_YEAR D_EXT_TERM_YEAR_SID, 
       nvl(DT.EXT_TERM_SID, 2147483646) D_EXT_TERM_SID, 
BEST_SUMM_TYPE_GPA_FLG, 
CLASS_RANK, 
CLASS_SIZE, 
CLASS_PERCENTILE, 
FROM_DT, 
TO_DT, 
LS_DATA_SOURCE, 
TRNSCR_FLG, 
TRNSCR_TYPE, 
TRNSCR_STATUS, 
TRNSCR_DT, 
CONVERTED_GPA, 
EXT_GPA, 
UNITS_ATTMPTD, 
UNITS_CMPLTD, 
UM_CONVERT_GPA, 
UM_CUM_CREDIT, 
UM_CUM_GPA, 
UM_CUM_QP, 
UM_EXT_ORG_CR, 
UM_EXT_ORG_QP, 
UM_EXT_ORG_GPA, 
UM_EXT_ORG_CNV_CR, 
UM_EXT_ORG_CNV_GPA, 
UM_EXT_ORG_CNV_QP, 
UM_GPA_EXCLUDE_FLG, 
UM_GPA_OVRD_FLG, 
UM_1_OVRD_HSGPA_FLG,
UM_EXT_OR_MTSC_GPA,            -- SMT-8300 Sep. 2019  
MS_CONVERT_GPA,                -- SMT-8300 Sep. 2019 
'N' LOAD_ERROR, 
'S' DATA_ORIGIN, 
SYSDATE CREATED_EW_DTTM, 
SYSDATE LASTUPD_EW_DTTM, 
1234 BATCH_SID
  from S
  left outer join PS_D_INSTITUTION I
    on S.INSTITUTION_CD = I.INSTITUTION_CD  
   and S.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P
    on S.PERSON_ID = P.PERSON_ID
   and S.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ORG O
    on S.EXT_ORG_ID = O.EXT_ORG_ID  
   and S.SRC_SYS_ID = O.SRC_SYS_ID
   and O.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ACAD_CAR EC
    on S.EXT_ACAD_CAR_ID = EC.EXT_ACAD_CAR_ID  
   and S.SRC_SYS_ID = EC.SRC_SYS_ID
   and EC.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_SUMM_TYP ES
    on S.EXT_SUMM_TYPE_ID = ES.EXT_SUMM_TYPE_ID
   and S.SRC_SYS_ID = ES.SRC_SYS_ID
   and ES.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ACAD_LVL EL
    on S.EXT_ACAD_LVL_ID = EL.EXT_ACAD_LVL_ID
   and S.SRC_SYS_ID = EL.SRC_SYS_ID
   and EL.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_RANK_TYP RT
    on S.ACAD_RANK_TYPE_ID = RT.ACAD_RANK_TYPE_ID
   and S.SRC_SYS_ID = RT.SRC_SYS_ID
   and RT.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_UNIT_TYP UT
    on S.ACAD_UNIT_TYPE_ID = UT.ACAD_UNIT_TYPE_ID
   and S.SRC_SYS_ID = UT.SRC_SYS_ID
   and UT.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_TERM ET
    on S.EXT_TERM_TYPE = ET.EXT_TERM_TYPE_ID
   and S.EXT_TERM_ID = ET.EXT_TERM_ID
   and S.SRC_SYS_ID = ET.SRC_SYS_ID
   and ET.DATA_ORIGIN <> 'D'
  left outer join PS_D_GPA_TYPE GT
    on S.INSTITUTION_CD = GT.INSTITUTION_CD         -- INSTITUTION_CD in PS_D_GPA_TYPE_NEW!!! 
   and S.GPA_TYPE_ID = GT.GPA_TYPE_ID
   and S.SRC_SYS_ID = GT.SRC_SYS_ID
   and GT.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ACAD_LVL DL
    on S.D_EXT_ACAD_LEVEL = DL.EXT_ACAD_LVL_ID
   and S.SRC_SYS_ID = DL.SRC_SYS_ID
   and DL.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_TERM DT
    on S.D_EXT_TERM_TYPE = DT.EXT_TERM_TYPE_ID
   and S.D_EXT_TERM = DT.EXT_TERM_ID
   and S.SRC_SYS_ID = DT.SRC_SYS_ID
   and DT.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_EXT_ACAD_SUMM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_EXT_ACAD_SUMM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_EXT_ACAD_SUMM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_EXT_ACAD_SUMM enable constraint PK_PS_F_EXT_ACAD_SUMM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_EXT_ACAD_SUMM');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_F_EXT_ACAD_SUMM_P;
/
