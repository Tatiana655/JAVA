import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.RC;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class IavorukExecutor implements ru.spbstu.pipeline.IExecutor {

    private IExecutable produser;//бесполезно? да. А зачем? "На будущее"(с).
    private IExecutable consumer;
    private Map<String, String> param = new HashMap<>();
    private final IavorukBaseGrammar baseGrammar = new IavorukBaseGrammar(new String[]{Param.COUNTB.getStringParam()});//куда и сколько двигать какие-то свои параметры
    private final Logger log;
    private static final int BYTE_SIZE = 8;
    private static final int ONES = 255;
    public IavorukExecutor(Logger logger)
    {
        log = logger;
    }
    public RC setConfig(String var1) {
        param = IavorukParser.GetParam(var1,baseGrammar,log);
        return IavorukParser.checkParam(param,baseGrammar,log);
    }
    public RC setConsumer(IExecutable var1)
    {
        if (var1 == null){
            log.info("ERROR: no consumer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        consumer = var1;
        return RC.CODE_SUCCESS;
    }
    public RC setProducer(IExecutable var1) //бесполезно? да. А зачем? "На будущее"(с).
    {
        if (var1 == null) {
            log.info("ERROR: no produser");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
        produser = var1;
        return RC.CODE_SUCCESS;
    }
    public byte shift(byte oneByte) {
        int count =Integer.parseInt(param.get(Param.COUNTB.getStringParam()));/*Integer.parseInt(param.get("COUNTB")) % 8*/
        if (count == 0) return oneByte;
        byte sym = oneByte;
        byte sym1 = oneByte;
        //right shift
        if (count > 0) {
            //сдвиг вправо с зануленим того, что там было
            //System.out.println(String.format("b>>> %8s", Integer.toBinaryString(sym & 0xFF)).replace(' ', '0'));
            sym = (byte) (sym >>> count);
            //System.out.println(String.format(">>> %8s", Integer.toBinaryString(sym & 0xFF)).replace(' ', '0'));
            //сдвиг влево содержимого
            sym1 = (byte) ((sym1 << (BYTE_SIZE - count)) );
        }
        //left shift
        else {
            count = -count;
            //сдвиг влево с зануленим(*без него) того, что там было
            sym = (byte) ((sym << count) );
            //сдвиг вправо содержимого
            sym1 = (byte) ((sym1 >>> (BYTE_SIZE - count)) & (ONES >> (BYTE_SIZE - count - 1)));
        }
        return (byte) (sym1 | sym);
    }
    public byte[] shiftAllBytes(byte[] oldBytes) {
        if (oldBytes == null) return null;
        int len = oldBytes.length;
        byte[] newByte = new byte[len];
        for (int i = 0; i < len; i++) {
            newByte[i] = shift(oldBytes[i]);
            //System.out.println("aft" + (newByte[i] & 0xFF));
        }
        return newByte;
    }

    public RC execute(byte[] var1)
    {
        if (var1==null) return RC.CODE_INVALID_ARGUMENT;
        //System.out.println("ee"+ Arrays.toString(var1));
        return consumer.execute(shiftAllBytes(var1));
    }
}
