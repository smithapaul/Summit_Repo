DROP VIEW CSSTG_OWNER.UMOL_TERM_S2_VW
/

--
-- UMOL_TERM_S2_VW  (View) 
--
CREATE OR REPLACE VIEW CSSTG_OWNER.UMOL_TERM_S2_VW
BEQUEATH DEFINER
AS 
select BB_SOURCE,PK1,NAME,DESCRIPTION_FORMAT_TYPE,SOURCEDID_SOURCE,SOURCEDID_ID,DATA_SRC_PK1,DURATION,START_DATE,END_DATE,DAYS_OF_USE,ROW_STATUS,AVAILABLE_IND,DTMODIFIED,DELETE_FLAG,INSERT_TIME,UPDATE_TIME from CSSTG_OWNER.UMOL_TERM_S2@SMTPROD
/
