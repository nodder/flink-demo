package name.cdd.study.flink.demo.sample.pojo;

public class CsvTableInfo
{
    private int id;
    private String name;
    private double score;
    public int getId()
    {
        return id;
    }
    public void setId(int id)
    {
        this.id = id;
    }
    public String getName()
    {
        return name;
    }
    public void setName(String name)
    {
        this.name = name;
    }
    public double getScore()
    {
        return score;
    }
    public void setScore(double score)
    {
        this.score = score;
    }
    @Override
    public String toString()
    {
        return "CsvTableInfo [id=" + id + ", name=" + name + ", score=" + score + "]";
    }

}
