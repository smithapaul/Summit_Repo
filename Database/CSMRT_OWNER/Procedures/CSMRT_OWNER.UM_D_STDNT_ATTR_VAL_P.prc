DROP PROCEDURE CSMRT_OWNER.UM_D_STDNT_ATTR_VAL_P
/

--
-- UM_D_STDNT_ATTR_VAL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_STDNT_ATTR_VAL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_STDNT_ATTR_VAL / UM_D_STDNT_ATTR_VAL_VW 
--Loads target table       -- UM_D_STDNT_ATTR_VAL
--UM_D_STDNT_ATTR_VAL      -- PS_D_INSTITUTION, PS_D_ACAD_CAR, PS_D_PERSON 
-- V01 4/12/2018           -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_STDNT_ATTR_VAL';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UM_D_STDNT_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UM_D_STDNT_ATTR_VAL';

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_STDNT_ATTR_VAL T
using (
 with Q1 as (  
select /*+ parallel(8) inline */
       INSTITUTION INSTITUTION_CD, STDNT_ATTR, SRC_SYS_ID, 
       DESCR STDNT_ATTR_LD, DESCRSHORT STDNT_ATTR_SD, 
       DATA_ORIGIN, 
       row_number() over (partition by INSTITUTION, STDNT_ATTR, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_STDNT_ATTR_TBL
 where DATA_ORIGIN <> 'D'),
       Q2 as (  
select /*+ parallel(8) inline */
       INSTITUTION INSTITUTION_CD, STDNT_ATTR, STDNT_ATTR_VALUE, SRC_SYS_ID, 
       DESCR STDNT_ATTR_VAL_LD, DESCRSHORT STDNT_ATTR_VAL_SD, 
       DATA_ORIGIN, 
       row_number() over (partition by INSTITUTION, STDNT_ATTR, STDNT_ATTR_VALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_STDNT_ATTR_VAL
 where DATA_ORIGIN <> 'D'),
       Q3 as (  
select /*+ parallel(8) inline */ 
       EMPLID PERSON_ID, ACAD_CAREER ACAD_CAR_CD, STDNT_CAR_NBR STDNT_CAR_NUM, STDNT_ATTR, STDNT_ATTR_VALUE, SRC_SYS_ID, 
       EFFDT, EFFSEQ, INSTITUTION INSTITUTION_CD, 
       DATA_ORIGIN, 
       row_number() over (partition by EMPLID, ACAD_CAREER, STDNT_CAR_NBR, STDNT_ATTR, STDNT_ATTR_VALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc, EFFSEQ desc) Q_ORDER
  from CSSTG_OWNER.PS_STDNT_ATTR_DTL),
       S as (
select /*+ parallel(8) inline */
       P.PERSON_ID, nvl(Q3.ACAD_CAR_CD,'-') ACAD_CAR_CD, nvl(Q3.STDNT_CAR_NUM,0) STDNT_CAR_NUM, nvl(Q3.STDNT_ATTR,'-') STDNT_ATTR, nvl(Q3.STDNT_ATTR_VALUE,'-') STDNT_ATTR_VALUE, P.SRC_SYS_ID, 
       nvl(Q3.INSTITUTION_CD,'-') INSTITUTION_CD, Q3.EFFDT, Q3.EFFSEQ, 
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID, nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, P.PERSON_SID, 
       nvl(Q1.STDNT_ATTR_SD,'-') STDNT_ATTR_SD, nvl(Q1.STDNT_ATTR_LD,'-') STDNT_ATTR_LD, 
       nvl(Q2.STDNT_ATTR_VAL_SD,'-') STDNT_ATTR_VAL_SD, nvl(Q2.STDNT_ATTR_VAL_LD,'-') STDNT_ATTR_VAL_LD, 
       max(case when Q3.INSTITUTION_CD = 'UMBOS' and Q3.STDNT_ATTR_VALUE = '2NDDEG' then 'Y' else 'N' end) 
           over (partition by Q3.PERSON_ID, Q3.ACAD_CAR_CD, Q3.STDNT_CAR_NUM, Q3.SRC_SYS_ID) UMBOS_UGRD_SECOND_DEGR_FLG,
       row_number() over (partition by Q3.PERSON_ID, Q3.ACAD_CAR_CD, Q3.STDNT_CAR_NUM, substr(Q3.STDNT_ATTR,-2,2), Q3.SRC_SYS_ID
                              order by Q3.DATA_ORIGIN desc, Q3.EFFDT desc, Q3.EFFSEQ desc, Q3.STDNT_ATTR, Q3.STDNT_ATTR_VALUE) ATTR_ORDER,
       least(P.DATA_ORIGIN,nvl(Q3.DATA_ORIGIN,'Z')) DATA_ORIGIN 
  from Q3 
  join PS_D_PERSON P        
    on Q3.PERSON_ID = P.PERSON_ID
   and Q3.SRC_SYS_ID = P.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  left outer join PS_D_INSTITUTION I     
    on Q3.INSTITUTION_CD = I.INSTITUTION_CD
   and Q3.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C       
    on Q3.INSTITUTION_CD = C.INSTITUTION_CD
   and Q3.ACAD_CAR_CD = C.ACAD_CAR_CD
   and Q3.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join Q1
    on Q3.INSTITUTION_CD = Q1.INSTITUTION_CD  
   and Q3.STDNT_ATTR = Q1.STDNT_ATTR
   and Q3.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q_ORDER = 1
  left outer join Q2
    on Q3.INSTITUTION_CD = Q2.INSTITUTION_CD  
   and Q3.STDNT_ATTR = Q2.STDNT_ATTR
   and Q3.STDNT_ATTR_VALUE = Q2.STDNT_ATTR_VALUE
   and Q3.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1)
select nvl(D.PERSON_ID, S.PERSON_ID) PERSON_ID,
       nvl(D.ACAD_CAR_CD, S.ACAD_CAR_CD) ACAD_CAR_CD,
       nvl(D.STDNT_CAR_NUM, S.STDNT_CAR_NUM) STDNT_CAR_NUM,
       nvl(D.STDNT_ATTR, S.STDNT_ATTR) STDNT_ATTR,
       nvl(D.STDNT_ATTR_VALUE, S.STDNT_ATTR_VALUE) STDNT_ATTR_VALUE,
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,
       decode(D.INSTITUTION_CD, S.INSTITUTION_CD, D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,
       decode(D.EFFSEQ, S.EFFSEQ, D.EFFSEQ, S.EFFSEQ) EFFSEQ,
       decode(D.INSTITUTION_SID, S.INSTITUTION_SID, D.INSTITUTION_SID, S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_CAR_SID, S.ACAD_CAR_SID, D.ACAD_CAR_SID, S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.PERSON_SID, S.PERSON_SID, D.PERSON_SID, S.PERSON_SID) PERSON_SID,
       decode(D.STDNT_ATTR_SD, S.STDNT_ATTR_SD, D.STDNT_ATTR_SD, S.STDNT_ATTR_SD) STDNT_ATTR_SD,
       decode(D.STDNT_ATTR_LD, S.STDNT_ATTR_LD, D.STDNT_ATTR_LD, S.STDNT_ATTR_LD) STDNT_ATTR_LD,
       decode(D.STDNT_ATTR_VAL_SD, S.STDNT_ATTR_VAL_SD, D.STDNT_ATTR_VAL_SD, S.STDNT_ATTR_VAL_SD) STDNT_ATTR_VAL_SD,
       decode(D.STDNT_ATTR_VAL_LD, S.STDNT_ATTR_VAL_LD, D.STDNT_ATTR_VAL_LD, S.STDNT_ATTR_VAL_LD) STDNT_ATTR_VAL_LD,
       decode(D.UMBOS_UGRD_SECOND_DEGR_FLG, S.UMBOS_UGRD_SECOND_DEGR_FLG, D.UMBOS_UGRD_SECOND_DEGR_FLG, S.UMBOS_UGRD_SECOND_DEGR_FLG) UMBOS_UGRD_SECOND_DEGR_FLG,
       decode(D.ATTR_ORDER, S.ATTR_ORDER, D.ATTR_ORDER, S.ATTR_ORDER) ATTR_ORDER,
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM
  from S
  left outer join CSMRT_OWNER.UM_D_STDNT_ATTR_VAL D
    on D.PERSON_SID <> 2147483646
   and D.PERSON_ID = S.PERSON_ID
   and D.ACAD_CAR_CD = S.ACAD_CAR_CD
   and D.STDNT_CAR_NUM = S.STDNT_CAR_NUM
   and D.STDNT_ATTR = S.STDNT_ATTR
   and D.STDNT_ATTR_VALUE = S.STDNT_ATTR_VALUE
   and D.SRC_SYS_ID = S.SRC_SYS_ID
) S
    on  (T.ACAD_CAR_CD = S.ACAD_CAR_CD
   and  T.PERSON_ID = S.PERSON_ID
   and  T.STDNT_CAR_NUM = S.STDNT_CAR_NUM
   and  T.STDNT_ATTR = S.STDNT_ATTR
   and  T.STDNT_ATTR_VALUE = S.STDNT_ATTR_VALUE
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
 when matched then update set
       T.INSTITUTION_CD = S.INSTITUTION_CD,
       T.EFFDT = S.EFFDT,
       T.EFFSEQ = S.EFFSEQ,
       T.INSTITUTION_SID = S.INSTITUTION_SID,
       T.ACAD_CAR_SID = S.ACAD_CAR_SID,
       T.PERSON_SID = S.PERSON_SID,
       T.STDNT_ATTR_SD = S.STDNT_ATTR_SD,
       T.STDNT_ATTR_LD = S.STDNT_ATTR_LD,
       T.STDNT_ATTR_VAL_SD = S.STDNT_ATTR_VAL_SD,
       T.STDNT_ATTR_VAL_LD = S.STDNT_ATTR_VAL_LD,
       T.UMBOS_UGRD_SECOND_DEGR_FLG = S.UMBOS_UGRD_SECOND_DEGR_FLG,
       T.ATTR_ORDER = S.ATTR_ORDER,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = SYSDATE
 where
       decode(T.INSTITUTION_CD,S.INSTITUTION_CD,0,1) = 1 or
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or
       decode(T.EFFSEQ,S.EFFSEQ,0,1) = 1 or
       decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or
       decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or
       decode(T.PERSON_SID,S.PERSON_SID,0,1) = 1 or
       decode(T.STDNT_ATTR_SD,S.STDNT_ATTR_SD,0,1) = 1 or
       decode(T.STDNT_ATTR_LD,S.STDNT_ATTR_LD,0,1) = 1 or
       decode(T.STDNT_ATTR_VAL_SD,S.STDNT_ATTR_VAL_SD,0,1) = 1 or
       decode(T.STDNT_ATTR_VAL_LD,S.STDNT_ATTR_VAL_LD,0,1) = 1 or
       decode(T.UMBOS_UGRD_SECOND_DEGR_FLG,S.UMBOS_UGRD_SECOND_DEGR_FLG,0,1) = 1 or
       decode(T.ATTR_ORDER,S.ATTR_ORDER,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1
  when not matched then
insert (
       T.PERSON_ID,
       T.ACAD_CAR_CD,
       T.STDNT_CAR_NUM,
       T.STDNT_ATTR,
       T.STDNT_ATTR_VALUE,
       T.SRC_SYS_ID,
       T.INSTITUTION_CD,
       T.EFFDT,
       T.EFFSEQ,
       T.INSTITUTION_SID,
       T.ACAD_CAR_SID,
       T.PERSON_SID,
       T.STDNT_ATTR_SD,
       T.STDNT_ATTR_LD,
       T.STDNT_ATTR_VAL_SD,
       T.STDNT_ATTR_VAL_LD,
       T.UMBOS_UGRD_SECOND_DEGR_FLG,
       T.ATTR_ORDER,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.PERSON_ID,
       S.ACAD_CAR_CD,
       S.STDNT_CAR_NUM,
       S.STDNT_ATTR,
       S.STDNT_ATTR_VALUE,
       S.SRC_SYS_ID,
       S.INSTITUTION_CD,
       S.EFFDT,
       S.EFFSEQ,
       S.INSTITUTION_SID,
       S.ACAD_CAR_SID,
       S.PERSON_SID,
       S.STDNT_ATTR_SD,
       S.STDNT_ATTR_LD,
       S.STDNT_ATTR_VAL_SD,
       S.STDNT_ATTR_VAL_LD,
       S.UMBOS_UGRD_SECOND_DEGR_FLG,
       S.ATTR_ORDER,
       S.DATA_ORIGIN,
       SYSDATE,
       SYSDATE);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_STDNT_ATTR_VAL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_STDNT_ATTR_VAL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.UM_D_STDNT_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_STDNT_ATTR_VAL';

update CSMRT_OWNER.UM_D_STDNT_ATTR_VAL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and not exists (select 1
                     from CSSTG_OWNER.PS_STDNT_ATTR_DTL S
                    where T.ACAD_CAR_CD = S.ACAD_CAREER
                     and  T.PERSON_ID = S.EMPLID
                     and  T.STDNT_CAR_NUM = S.STDNT_CAR_NBR
                     and  T.STDNT_ATTR = S.STDNT_ATTR
                     and  T.STDNT_ATTR_VALUE = S.STDNT_ATTR_VALUE
                     and  T.SRC_SYS_ID = S.SRC_SYS_ID
                     and  S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_STDNT_ATTR_VAL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_STDNT_ATTR_VAL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

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

END UM_D_STDNT_ATTR_VAL_P;
/
