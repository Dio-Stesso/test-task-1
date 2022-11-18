package aa.intellias.testtask.collection;

import lombok.Data;

@Data
public class DataRow<K extends Comparable<K>, V> {
    private K key;
    private V value;

    public DataRow() {
    }

    public DataRow(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
