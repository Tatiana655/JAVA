import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RC;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class IavorukReader implements IReader {
    //поле консумера
    //params
    //log
    //baseGrammar
    private IExecutable consumer;
    private Map<String, String> param = new HashMap<>();
    private final IavorukBaseGrammar baseGrammar = new IavorukBaseGrammar(new String[]{Param.BUFFSIZE.getStringParam()});
    private Logger log = Logger.getLogger("MyLog");
    private FileInputStream fileInputStream;

    public IavorukReader(Logger logger)//файл лога мб
    {
        log = logger;
    }

    public RC setConfig(String var1)
    {
        param = IavorukParser.GetParam(var1,baseGrammar,log);
        RC rc = IavorukParser.checkParam(param,baseGrammar,log);
        if (rc!=RC.CODE_SUCCESS) return rc;
        if (Integer.parseInt(param.get(Param.BUFFSIZE.getStringParam())) <=0 ) return RC.CODE_CONFIG_SEMANTIC_ERROR;
        return RC.CODE_SUCCESS;
    }
    public RC setInputStream(FileInputStream var1)
    {
        if (var1 == null) return RC.CODE_INVALID_INPUT_STREAM;
        fileInputStream = var1;
        return RC.CODE_SUCCESS;
    }
    public RC setConsumer(IExecutable var1)
    {
        if (var1 == null) return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        consumer =  var1;
        return RC.CODE_SUCCESS;
    }

    public RC setProducer(IExecutable var1)//бесполезна? да. А зачем? "На будущее"(с).
    {
        return RC.CODE_SUCCESS;
    }

    public RC execute(byte[] var1)
    {
        //Считай кусок размером с буффер и перекить воркеру
        int buffSize = Integer.parseInt(param.get("BUFFSIZE"));
        byte[] byteStream = new byte[buffSize];
        int k = 0;
        while (true) {
            try {
                k = (fileInputStream != null) ? (fileInputStream.read(byteStream)) : -1;
                if (k == -1) break;
                if (consumer.execute(byteStream) !=RC.CODE_SUCCESS)
                    return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
                Arrays.fill(byteStream, (byte)0);
            } catch (Exception e) {
                log.info("ERROR: wrong inputstream");
                return RC.CODE_FAILED_TO_READ;
            }
        }
        if (fileInputStream!= null) {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                log.info("ERROR: cannot close input file, wrong stream");
                return RC.CODE_INVALID_INPUT_STREAM;
            } catch (Exception e1) {
                log.info("ERROR: cannot close input file");
                return RC.CODE_INVALID_INPUT_STREAM;
            }
        }
        return RC.CODE_SUCCESS;
    }
}
