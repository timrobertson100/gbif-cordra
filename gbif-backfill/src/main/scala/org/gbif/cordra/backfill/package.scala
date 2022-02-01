package org.gbif.cordra

package object backfill {

  // SQL to extract data for testing
  val SQL_OCCURRENCE = """
SELECT
  gbifId, datasetKey, basisOfRecord, publishingorgkey, datasetName, publisher,
  kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, acceptedTaxonKey, taxonKey,
  scientificName, acceptedScientificName, kingdom, phylum, order_ AS order, family, genus, species, genericName, specificEpithet, taxonRank,
  typeStatus, preparations,
  decimalLatitude, decimalLongitude, countryCode,
  year, month, day, from_unixtime(floor(eventDate/1000)) AS eventDate,
  recordNumber, fieldNumber, occurrenceID, otherCatalogNumbers, institutionCode, collectionCode, catalogNumber,
  recordedBy,
  ext_multimedia
FROM occurrence
WHERE basisOfRecord='PRESERVED_SPECIMEN'
"""

  //AND gbifId=699168

}
