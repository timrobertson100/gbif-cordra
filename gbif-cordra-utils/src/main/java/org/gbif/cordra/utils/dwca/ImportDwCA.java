package org.gbif.cordra.utils.dwca;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import net.cnri.cordra.api.CordraClient;
import net.cnri.cordra.api.CordraException;
import net.cnri.cordra.api.CordraObject;
import net.cnri.cordra.api.TokenUsingHttpCordraClient;
import net.cnri.cordra.util.GsonUtility;
import org.gbif.dwc.Archive;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;

/**
 * Takes a DwC-A and loads it into Cordra. This is intended for bootstrapping Cordra for quick
 * exploration and does not pay particular attention to the data model. See the sample
 * import-config.json for configuration.
 */
public class ImportDwCA {
  private static Gson GSON = GsonUtility.getGson();

  public static void main(String[] args) throws IOException, CordraException {
    Reader reader = Files.newBufferedReader(Paths.get(args[0]));
    Map<String, String> params = GSON.fromJson(reader, Map.class);

    Archive dwcArchive =
        DwcFiles.fromCompressed(
            Paths.get(params.get("dwcaPath")), Files.createTempDirectory("dwca"));

    CordraClient cordra =
        new TokenUsingHttpCordraClient(
            params.get("baseUri"), params.get("username"), params.get("password"));

    for (Record rec : dwcArchive.getCore()) {
      System.out.println(
          String.format(
              "Loading record[%s], scientificName[%s]",
              rec.id(), rec.value(DwcTerm.scientificName)));

      JsonObject doc = new JsonObject();
      doc.addProperty("scientificName", rec.value(DwcTerm.scientificName));
      CordraObject co = new CordraObject();
      co.id = "test/" + rec.id();
      co.setContent(doc);
      co.type = params.get("schemaName");
      cordra.create(co);
    }
  }
}
