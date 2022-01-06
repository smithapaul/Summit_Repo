CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_ACCOM_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_D_PERSON_ACCOM.
--
 --V01  SMT-xxxx 07/05/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_ACCOM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_ACCOM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_ACCOM');

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_ACCOM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_ACCOM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_ACCOM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_ACCOM';				
insert /*+ append */ into UM_D_PERSON_ACCOM
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90'), 
REQ as (
select EMPLID,           
       EMPL_RCD,         
       ACCOMMODATION_ID,  
       SRC_SYS_ID,        
       DT_REQUESTED,      
       RESPONSIBLE_ID,     
       REQUEST_STATUS,   
       STATUS_DT,         
       substr(to_char(trim(COMMENTS)),1,4000) COMMENTS,          
       row_number () over (partition by EMPLID, EMPL_RCD, SRC_SYS_ID
                             order by ACCOMMODATION_ID desc) REQ_ORDER
  from CSSTG_OWNER.PS_ACCOM_REQUEST
 where DATA_ORIGIN <> 'D'),
TYP as (
select ACCOMMODATION_TYPE, 
       EFFDT, 
       SRC_SYS_ID, 
       EFF_STATUS, 
       DESCRSHORT ACCOMMODATION_TYPE_SD,
       DESCR ACCOMMODATION_TYPE_LD, 
       row_number () over (partition by ACCOMMODATION_TYPE, SRC_SYS_ID
                             order by EFFDT desc) TYPE_ORDER
  from CSSTG_OWNER.PS_ACCOM_TYPE_TBL
 where DATA_ORIGIN <> 'D')
select P.PERSON_SID,
       REQ.EMPLID,           
       REQ.EMPL_RCD,         
       REQ.ACCOMMODATION_ID,
       OPT.ACCOMMODATION_OPT,   
       REQ.SRC_SYS_ID,
       case when substr(OPT.ACCOMMODATION_TYPE,1,1) = 'B' then 'UMBOS'
            when substr(OPT.ACCOMMODATION_TYPE,1,1) = 'D' then 'UMDAR'
            when substr(OPT.ACCOMMODATION_TYPE,1,1) = 'L' then 'UMLOW'
            else '-'
        end INSTITUTION_CD,    
       nvl(P2.PERSON_SID,2147483646) PERSON_RESP_SID,
       REQ.DT_REQUESTED,      
       REQ.REQUEST_STATUS,   
       nvl(X1.XLATSHORTNAME,'-') REQUEST_STATUS_SD, 
       nvl(X1.XLATLONGNAME,'-') REQUEST_STATUS_LD, 
       REQ.STATUS_DT REQ_STATUS_DT,         
       OPT.ACCOMMODATION_TYPE,
       nvl(TYP.ACCOMMODATION_TYPE_SD,'-') ACCOMMODATION_TYPE_SD,
       nvl(TYP.ACCOMMODATION_TYPE_LD,'-') ACCOMMODATION_TYPE_LD,
       OPT.ACCOM_STATUS,
       nvl(X2.XLATSHORTNAME,'-') ACCOM_STATUS_SD, 
       nvl(X2.XLATLONGNAME,'-') ACCOM_STATUS_LD,
       OPT.STATUS_DT ACCOM_STATUS_DT, 
       REQ.COMMENTS REQ_COMMENTS,
       substr(to_char(trim(OPT.DESCRLONG)),1,4000) ACCOM_DESCRLONG,
          'N' LOAD_ERROR,
          'S' DATA_ORIGIN,
          sysdate CREATED_EW_DTTM,
          sysdate LASTUPD_EW_DTTM,
          1234 BATCH_SID
  from REQ
  join CSSTG_OWNER.PS_ACCOM_OPTION OPT
    on REQ.EMPLID = OPT.EMPLID 
   and REQ.EMPL_RCD = OPT.EMPL_RCD
   and REQ.ACCOMMODATION_ID = OPT.ACCOMMODATION_ID
   and REQ.SRC_SYS_ID = OPT.SRC_SYS_ID
   and OPT.DATA_ORIGIN <> 'D'
  join CSMRT_OWNER.PS_D_PERSON P
    on REQ.EMPLID = P.PERSON_ID
   and REQ.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P2
    on REQ.RESPONSIBLE_ID = P2.PERSON_ID
   and REQ.SRC_SYS_ID = P2.SRC_SYS_ID
  left outer join XL X1
    on X1.FIELDNAME = 'REQUEST_STATUS'
   and X1.FIELDVALUE = REQ.REQUEST_STATUS 
   and X1.SRC_SYS_ID = REQ.SRC_SYS_ID
  left outer join XL X2
    on X2.FIELDNAME = 'ACCOM_STATUS'
   and X2.FIELDVALUE = OPT.ACCOM_STATUS 
   and X2.SRC_SYS_ID = OPT.SRC_SYS_ID
  left outer join TYP
    on OPT.ACCOMMODATION_TYPE = TYP.ACCOMMODATION_TYPE
   and OPT.SRC_SYS_ID = TYP.SRC_SYS_ID
   and TYP.TYPE_ORDER = 1
 where REQ.REQ_ORDER = 1
UNION ALL 
select 
		2147483646 PERSON_SID, 
		'-' EMPLID, 
		0 EMPL_RCD, 
		0 ACCOMMODATION_ID, 
		0 ACCOMMODATION_OPT, 
		'CS90' SRC_SYS_ID, 
		'-' INSTITUTION_CD, 
		2147483646 PERSON_RESP_SID, 
		NULL DT_REQUESTED, 
		NULL REQUEST_STATUS, 
		NULL REQUEST_STATUS_SD, 
		NULL REQUEST_STATUS_LD, 
		NULL REQ_STATUS_DT, 
		NULL ACCOMMODATION_TYPE, 
		NULL ACCOMMODATION_TYPE_SD, 
		NULL ACCOMMODATION_TYPE_LD, 
		NULL ACCOM_STATUS, 
		NULL ACCOM_STATUS_SD, 
		NULL ACCOM_STATUS_LD, 
		NULL ACCOM_STATUS_DT, 
		NULL REQ_COMMENTS, 
		NULL ACCOM_DESCRLONG, 
		'N' LOAD_ERROR, 
		'S' DATA_ORIGIN, 
		sysdate CREATED_EW_DTTM, 
		sysdate LASTUPD_EW_DTTM, 
		1234 BATCH_SID
  from DUAL 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;
/*
Insert DEfault Row
*/



strMessage01    := '# of UM_D_PERSON_ACCOM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ACCOM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ACCOM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_ACCOM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_ACCOM');

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

END UM_D_PERSON_ACCOM_P;
/
