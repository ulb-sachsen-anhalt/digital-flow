<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<xsl:stylesheet xmlns:mets="http://www.loc.gov/METS/"
    xmlns:mods="http://www.loc.gov/mods/v3"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:svrl="http://purl.oclc.org/dsdl/svrl" version="2.0">
    <xsl:variable name="isIssueOrAdditional">
        <xsl:value-of select="boolean(//mets:div[@TYPE='day']) and not(//mets:div[@TYPE='day']/mets:div/mets:mptr)"/>
    </xsl:variable>
    <xsl:template match="//mets:mets">
        <svrl:fired-rule>
            <xsl:variable name="metsDayDate">
                <xsl:value-of select="//mets:div[@TYPE='day']/@ORDERLABEL"/>
            </xsl:variable>
            <xsl:variable name="modsPublDate">
                <xsl:value-of select="//mets:dmdSec[@ID]/mets:mdWrap/mets:xmlData/mods:mods/mods:originInfo[@eventType='publication']/mods:dateIssued/text()"/>
            </xsl:variable>
            <xsl:choose>
                <xsl:when test="$isIssueOrAdditional and not($metsDayDate=$modsPublDate)">
                    <svrl:failed-assert>
                        <xsl:attribute name="id">date_mets_to_mods</xsl:attribute>
                        <xsl:attribute name="role">fatal</xsl:attribute>
                        <svrl:text>Logisches Datum passt nicht zu Publikationsdatum: <xsl:value-of select="$metsDayDate"/> != <xsl:value-of select="$modsPublDate"/></svrl:text>
                    </svrl:failed-assert>
                </xsl:when>
                <xsl:otherwise>Datumsangaben passen: <xsl:value-of select="$metsDayDate"/> = <xsl:value-of select="$modsPublDate"/></xsl:otherwise>
            </xsl:choose>
        </svrl:fired-rule>
    </xsl:template>
</xsl:stylesheet>
