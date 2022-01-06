CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_GRADE_RSTR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table        -- UM_F_STDNT_GRADE_RSTR
-- V01 10/24/2018     -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_GRADE_RSTR';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_GRADE_RSTR');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR disable constraint PK_UM_F_STDNT_GRADE_RSTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';				
insert /*+ append */ into UM_F_STDNT_GRADE_RSTR 
 with XL as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D')
select /*+ parallel(8) */
       V.STRM AS TERM_CD,
       V.CLASS_NBR AS CLASS_NBR,
       V.GRD_RSTR_TYPE_SEQ,
       V.EMPLID AS PERSON_ID,
       ACAD_CAREER AS ACAD_CAR_CD,
       V.SRC_SYS_ID,
       I.INSTITUTION_CD,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
       nvl(D.TERM_SID, 2147483646) TERM_SID,
       nvl(P.PERSON_SID, 2147483646) PERSON_SID,
       nvl(G.GRD_RSTR_TYPE_SID,2147483646) GRD_RSTR_TYPE_SID,
       V.BLIND_GRADING_ID,
       V.LAST_NAME_SRCH,
       V.FIRST_NAME_SRCH,
       V.CRSE_GRADE_INPUT,
       V.RQMNT_DESIGNTN_GRD,
       nvl(X1.XLATSHORTNAME,'-') RQMNT_DESIGNTN_GRD_SD, 
       nvl(X1.XLATLONGNAME,'-') RQMNT_DESIGNTN_GRD_LD,
       V.TSCRPT_NOTE_ID,
       V.TSCRPT_NOTE_EXISTS,
       V.GRADE_ROSTER_STAT,
       nvl(X2.XLATSHORTNAME,'-') GRADE_ROSTER_STAT_SD, 
       nvl(X2.XLATLONGNAME,'-') GRADE_ROSTER_STAT_LD,
       V.INSTRUCTOR_ID,
       V.GRADING_SCHEME,
       V.GRADING_BASIS_ENRL,
       V.DYN_CLASS_NBR ,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CSSTG_OWNER.PS_GRADE_ROSTER V
  left outer join XL X1
    on X1.FIELDNAME = 'RQMNT_DESIGNTN_GRD'
   and X1.FIELDVALUE = V.RQMNT_DESIGNTN_GRD 
   and X1.SRC_SYS_ID = V.SRC_SYS_ID
   and X1.X_ORDER = 1
  left outer join XL X2
    on X2.FIELDNAME = 'GRADE_ROSTER_STAT'
   and X2.FIELDVALUE = V.GRADE_ROSTER_STAT 
   and X2.SRC_SYS_ID = V.SRC_SYS_ID
   and X2.X_ORDER = 1
  left outer join CSMRT_OWNER.UM_D_GRD_RSTR_TYPE G
    on V.STRM=G.TERM_CD
   and V.CLASS_NBR = G.CLASS_NBR
   and V.SRC_SYS_ID = G.SRC_SYS_ID
   and V.GRD_RSTR_TYPE_SEQ = G.GRD_RSTR_TYPE_SEQ
   and G.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on V.INSTITUTION = I.INSTITUTION_CD
   and V.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer JOIN CSMRT_OWNER.PS_D_PERSON P
    on V.EMPLID = P.PERSON_ID  
   and V.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_TERM D 	
    on D.INSTITUTION_CD = V.INSTITUTION
   and D.ACAD_CAR_CD = V.ACAD_CAREER
   and D.TERM_CD = V.STRM
   and D.SRC_SYS_ID = V.SRC_SYS_ID 
   and D.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on V.ACAD_CAREER = C.ACAD_CAR_CD 
   and V.INSTITUTION = C.INSTITUTION_CD	
   and V.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
 where V.DATA_ORIGIN <> 'D' 
 ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_GRADE_RSTR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRADE_RSTR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );
strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRADE_RSTR enable constraint PK_UM_F_STDNT_GRADE_RSTR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_GRADE_RSTR');

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

END UM_F_STDNT_GRADE_RSTR_P;
/
