package sort.test.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//自定义一个javabean类，作为Key时，可以对多个字段操作
public class Person implements WritableComparable<Person> {
    private String name;
    private int age;
    private int salary;

    //反序列化时，会反射调用空参构造，所以必须要有
    public Person(){
            }

    public Person(String name,int age,int salary){
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    public int getAge() {
        return age;
    }

    public int getSalary() {
        return salary;
    }

    public String getName() {
        return name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeInt(salary);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.salary = dataInput.readInt();
    }

    //根据返回值大于0、等于0、小于0再调用数组sort排序（默认升序），
    @Override
    public int compareTo(Person o) {
        int result = this.salary - o.salary; //大于0，表示，左边的比右边大，返回值也大于0，交换位置，升序排序
                                             //小于0，表示，左边的比右边小，返回值也小于0，不交换位置，升序排序
        if (result != 0){
            return -result;//大于0则返回小于0，降序
        }else {
            return this.age - o.age;//总结：直接return左边-右边的值，就是升序
        }

    }

    @Override
    public String toString() {
        return this.salary + " " + this.age + " " + this.name;
    }
}
