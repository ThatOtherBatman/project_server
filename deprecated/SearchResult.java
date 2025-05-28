package com.teza.common.tardis.deprecated;

//import org.codehaus.jackson.annotate.JsonAutoDetect;
//import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

import com.teza.common.tardis.Doc;
import com.teza.common.tardis.FileLocation;
import com.teza.common.tardis.TardisClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: tom
 * Date: 10/10/16
 * Time: 12:54 PM
 */
public class SearchResult
{
    private final Map<String, Doc> docs = new HashMap<String, Doc>();
    private final Map<String, FileLocation> locations = new HashMap<String, FileLocation>();
    private final List<FileIndex> indices = new ArrayList<FileIndex>();

    public Map<String, Doc> getDocs()
    {
        return docs;
    }

    public Map<String, FileLocation> getLocations()
    {
        return locations;
    }

    public List<FileIndex> getIndices()
    {
        return indices;
    }

    public void add(TardisClient client, String docUuid, String hierOrder, String parentDocUuid, String parentHierOrder, String startTs, String endTs,
                    String fileUuid, String validFromTs, String validToTs, String content, String dataFromTs, String dataToTs, String fileMeta, String locationUuids)
    {
        if (!docs.containsKey(docUuid))
        {
            docs.put(docUuid, client.getDoc(docUuid));
        }
        String[] locUuids = parseLocationUuids(locationUuids);
        if (locUuids != null)
        {
            for (String l : locUuids)
            {
                if (!locations.containsKey(l))
                {
                    locations.put(l, client.getLocation(l, false));
                }
            }
        }
        FileIndex i = new FileIndexImpl(docUuid, hierOrder, parentDocUuid, parentHierOrder, startTs, endTs,
                fileUuid, dataFromTs, dataToTs, content, validFromTs, validToTs, fileMeta, locationUuids);
        indices.add(i);
    }

    private String[] parseLocationUuids(String locationUuids)
    {
        if (locationUuids == null || locationUuids.isEmpty())
        {
            return null;
        }

        //noinspection UnnecessaryLocalVariable
        String[] uuids = locationUuids.substring(1, locationUuids.length() - 1).split(",", -1);
        return uuids;
    }

    public static class FileIndexImpl implements FileIndex
    {
        private String docUuid, hierOrder, parentDocUuid, parentHierOrder, startTs, endTs;
        private String fileUuid, dataFromTs, dataToTs, content, validFromTs, validToTs, fileMeta, locationUuids;

        public FileIndexImpl(String docUuid, String hierOrder, String parentDocUuid, String parentHierOrder, String startTs, String endTs,
                             String fileUuid, String dataFromTs, String dataToTs, String content, String validFromTs, String validToTs, String fileMeta, String locationUuids)
        {
            this.docUuid = docUuid;
            this.hierOrder = hierOrder;
            this.parentDocUuid = parentDocUuid;
            this.parentHierOrder = parentHierOrder;
            this.startTs = startTs;
            this.endTs = endTs;
            this.fileUuid = fileUuid;
            this.dataFromTs = dataFromTs;
            this.dataToTs = dataToTs;
            this.content = content;
            this.validFromTs = validFromTs;
            this.validToTs = validToTs;
            this.fileMeta = fileMeta;
            this.locationUuids = locationUuids;
        }

        public String getDocUuid()
        {
            return docUuid;
        }

        public String getHierOrder()
        {
            return hierOrder;
        }

        public String getParentDocUuid()
        {
            return parentDocUuid;
        }

        public String getParentHierOrder()
        {
            return parentHierOrder;
        }

        public String getStartTs()
        {
            return startTs;
        }

        public String getEndTs()
        {
            return endTs;
        }

        public String getFileUuid()
        {
            return fileUuid;
        }

        public String getDataFromTs()
        {
            return dataFromTs;
        }

        public String getDataToTs()
        {
            return dataToTs;
        }

        public String getContent()
        {
            return content;
        }

        public String getValidFromTs()
        {
            return validFromTs;
        }

        public String getValidToTs()
        {
            return validToTs;
        }

        public String getFileMeta()
        {
            return fileMeta;
        }

        public String getLocationUuids()
        {
            return locationUuids;
        }
    }
}
