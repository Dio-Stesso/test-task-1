package aa.intellias.testtask.collection;

import lombok.Data;

@Data
public class JoinedDataRow<K extends Comparable<K>, V1, V2> implements Comparable<K> {
    private K key;
    private V1 leftValue;
    private V2 rightValue;

    public JoinedDataRow() {
    }

    public JoinedDataRow(K key, V1 leftValue, V2 rightValue) {
        this.key = key;
        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }

    @Override
    public int compareTo(K key) {
        return key.compareTo(this.key);
    }
}
