DROP FUNCTION CSMRT_OWNER.ISDATE
/

--
-- ISDATE  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.isdate(p_inDate varchar2, p_format varchar2) return number
as
 v_dummy date;
begin
 select to_date(p_inDate,p_format) into v_dummy from dual;
 return 0;
exception
 when others then return 1;
end isdate; 
 
/
