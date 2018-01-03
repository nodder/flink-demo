package name.cdd.study.flink.demo.sample.pojo;

public class CodeTimePair //可以使用Tuple2代替
{
    private String code;
    private long time;
    
    public CodeTimePair() {}
    
    public CodeTimePair(String[] components)
    {
        this.code = components[0];
        this.time = Long.parseLong(components[1]);
    }
    public String getCode()
    {
        return code;
    }
    public void setCode(String code)
    {
        this.code = code;
    }
    public long getTime()
    {
        return time;
    }
    public void setTime(long time)
    {
        this.time = time;
    }
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((code == null) ? 0 : code.hashCode());
        result = prime * result + (int)(time ^ (time >>> 32));
        return result;
    }
    @Override
    public boolean equals(Object obj)
    {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        CodeTimePair other = (CodeTimePair)obj;
        if(code == null)
        {
            if(other.code != null)
                return false;
        }
        else if(!code.equals(other.code))
            return false;
        if(time != other.time)
            return false;
        return true;
    }
}
