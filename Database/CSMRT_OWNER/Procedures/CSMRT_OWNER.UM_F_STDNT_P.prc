CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table          -- UM_F_STDNT
--UM_F_STDNT       -- PS_D_INSTITUTION, PS_D_ACAD, PS_D_PERSON;
--V01 12/4/2018        -- srikanth ,pabbu converted to proc from sql 

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT disable constraint PK_UM_F_STDNT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT';				
insert /*+ append */ into UM_F_STDNT 
WITH A AS (
SELECT /*+ INLINE PARALLEL(8) */ DISTINCT 
       INSTITUTION, ACAD_CAREER, EMPLID, SRC_SYS_ID, 1 ADM_CNT, 0 SR_CNT, 0 PRSPCT_CNT
  FROM CSSTG_OWNER.PS_ADM_APPL_PROG
 WHERE DATA_ORIGIN <> 'D'
 UNION ALL
SELECT /*+ INLINE PARALLEL(8) */ DISTINCT 
       INSTITUTION, ACAD_CAREER, EMPLID, SRC_SYS_ID, 0 ADM_CNT, 1 SR_CNT, 0 PRSPCT_CNT
  FROM CSSTG_OWNER.PS_ACAD_PROG
 WHERE DATA_ORIGIN <> 'D' 
  UNION ALL
 SELECT /*+ INLINE PARALLEL(8) */ DISTINCT 
        INSTITUTION, ACAD_CAREER, EMPLID, SRC_SYS_ID, 0 ADM_CNT, 0 SR_CNT, 1 PRSPCT_CNT
   FROM CSSTG_OWNER.PS_ADM_PRSPCT_CAR
  WHERE DATA_ORIGIN <> 'D'
)  
SELECT /*+ INLINE PARALLEL(8) */ 
       A.INSTITUTION INSTITUTION_CD,
	   A.ACAD_CAREER ACAD_CAR_CD, 
	   A.EMPLID PERSON_ID, 
	   A.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,  	   
       SUM(A.ADM_CNT) ADM_CNT, 
	   SUM(A.SR_CNT) SR_CNT, 
	   SUM(A.PRSPCT_CNT) PRSPCT_CNT,
	   'N' LOAD_ERROR,
	   'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM,
	   '1234' BATCH_SID
  FROM A
--  left outer join PS_D_INSTITUTION I
  join PS_D_INSTITUTION I   -- Nov 2019 
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <>'D'
--  left outer join PS_D_ACAD_CAR C
  join PS_D_ACAD_CAR C      -- Nov 2019 
    on A.INSTITUTION = C.INSTITUTION_CD
   and A.ACAD_CAREER = C.ACAD_CAR_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D' 
--  left outer join PS_D_PERSON P
  join UM_D_PERSON_AGG P    -- Nov 2019 
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D' 
 GROUP BY A.INSTITUTION, A.EMPLID, A.ACAD_CAREER, A.SRC_SYS_ID,I.INSTITUTION_SID,C.ACAD_CAR_SID,P.PERSON_SID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT enable constraint PK_UM_F_STDNT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT');

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

END UM_F_STDNT_P;
/
