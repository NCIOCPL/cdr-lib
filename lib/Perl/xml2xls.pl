#!/usr/bin/perl

#----------------------------------------------------------------------
#
# $Id: xml2xls.pl,v 1.3 2008-09-15 19:11:21 bkline Exp $
#
# Convert workbooks generated by CDR Python module ExcelWriter
# from Excel 2003 XML format to binary Excel '97 XLS format.
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2007/06/18 18:26:23  kidderc
# 3309. Added big argument to writing excel files. Used for large excel files.
#
# Revision 1.1  2006/11/07 15:03:19  bkline
# Creates binary XLS from Excel 2003 XML files.
#
#----------------------------------------------------------------------
use strict;
use XML::DOM;
use Spreadsheet::WriteExcel;
use Spreadsheet::WriteExcel::Big;
use Data::Dumper;

my ($inName, $outName, $big) = @ARGV;
my $parser = new XML::DOM::Parser;
my $doc = $parser->parsefile($inName);
my $book = Spreadsheet::WriteExcel->new($outName);
if ($big eq 'True') {
    $book = Spreadsheet::WriteExcel::Big->new($outName);
}
    
my $defaultStyle = $book->add_format;

my %styles;
my %mergeStyles;
my %colors = (
    "black"   =>  8,
    "blue"    => 12,
    "brown"   => 16,
    "cyan"    => 15,
    "gray"    => 23,
    "green"   => 17,
    "lime"    => 11,
    "magenta" => 14,
    "navy"    => 18,
    "orange"  => 53,
    "pink"    => 33,
    "purple"  => 20,
    "red"     => 10,
    "silver"  => 22,
    "white"   =>  9,
    "yellow"  => 13
);
my $nextColorIndex = 19;

my %patterns = (
    'Solid'                 =>  1,
    'Gray50'                =>  2,
    'Gray75'                =>  3,
    'Gray25'                =>  4,
    'HorzStripe'            =>  5,
    'VertStripe'            =>  6,
    'ReverseDiagStripe'     =>  7,
    'DiagStripe'            =>  8,
    'DiagCross'             =>  9,
    'ThickDiagCross'        => 10,
    'ThinHorzStripe'        => 11,
    'ThinVertStripe'        => 12,
    'ThinReverseDiagStripe' => 13,
    'ThinDiagStripe'        => 14,
    'ThinHorzCross'         => 15,
    'ThinDiagCross'         => 16,
    'Gray125'               => 17,
    'Gray0625'              => 18,
);

my %verticalAlignments = (
    "Top" => "top",
    "Center" => "vcenter",
    "Bottom" => "bottom",
    "Justify" => "vjustify"
#   "Distributed" => "[not mapped]"
);

my %horizontalAlignments = (
    "Left"                  => "left",
    "Center"                => "center",
    "Right"                 => "right",
    "Fill"                  => "fill",
    "Justify"               => "justify",
    "CenterAcrossSelection" => "center_across"
);

sub getColor {
    my $val = shift;
    my $color = "";
    if (exists $colors{$val}) {
        $color = $colors{$val};
    }
    elsif ($val =~ /^\#[0-9A-Fa-f]{6}$/) {
        while (exists $colors{$nextColorIndex}) {
            $nextColorIndex++;
            return if $nextColorIndex > 63;
        }
        if ($nextColorIndex < 64) {
            $color = $book->set_custom_color($nextColorIndex, $val);
            $nextColorIndex++;
        }
    }
    $color;
}

sub getBorderSetting {
    my ($weight, $lineStyle) = @_;
    my $setting = 0;
    if ($lineStyle eq "Continuous") {
        if ($weight == 1) {
            $setting = 1;
        }
        elsif ($weight == 2) {
            $setting = 2;
        }
        elsif ($weight == 2) {
            $setting = 5;
        }
        else {
            $setting = 7;
        }
    }
    elsif ($lineStyle eq "Dash") {
        if ($weight == 2) {
            $setting = 8;
        }
        else {
            $setting = 3;
        }
    }
    elsif ($lineStyle eq "Dot") {
        $setting = 4;
    }
    elsif ($lineStyle eq "Double") {
        $setting = 6;
    }
    elsif ($lineStyle eq "DashDot") {
        if ($weight == 1) {
            $setting = 9;
        }
        else {
            $setting = 10;
        }
    }
    elsif ($lineStyle eq "DashDotDot") {
        if ($weight == 1) {
            $setting = 11;
        }
        else {
            $setting = 12;
        }
    }
    elsif ($lineStyle eq "SlantDashDot") {
        $setting = 13;
    }
    $setting;
}

sub setBorder {
    my ($style, $node) = @_;
    my $weight = $node->getAttribute("ss:Weight");
    my $position = $node->getAttribute("ss:Position");
    my $lineStyle = $node->getAttribute("ss:LineStyle");
    my $setting = getBorderSetting($weight, $lineStyle);
    if ($setting > 1 || $setting == 0) {
        if ($position eq "Bottom") {
            $style->set_bottom($setting);
        }
        elsif ($position eq "Top") {
            $style->set_top($setting);
        }
        elsif ($position eq "Left") {
            $style->set_left($setting);
        }
        elsif ($position eq "Right") {
            $style->set_right($setting);
        }
    }
}
                        
sub addStyle {
    my ($book, $node) = @_;
    my $styleId = $node->getAttribute("ss:ID");
    my $style = $book->add_format;
    my $mergeStyle = $book->add_format;
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeName eq "Font") {
            my $color = $child->getAttribute("ss:Color");
            my $pattern = $child->getAttribute("ss:Pattern");
            my $underline = $child->getAttribute("ss:Underline");
            my $fontName = $child->getAttribute("ss:FontName");
            my $family = $child->getAttribute("ss:Family");
            my $bold = $child->getAttribute("ss:Bold");
            my $italic = $child->getAttribute("ss:Italic");
            my $size = $child->getAttribute("ss:Size");
            if ($color) {
                my $code = getColor($color);
                if ($code) {
                    $style->set_color($code);
                    $mergeStyle->set_color($code);
                }
            }
            if ($fontName) {
                $style->set_font($fontName);
                $mergeStyle->set_font($fontName);
            }
            elsif ($family) {
                $style->set_font($family);
                $mergeStyle->set_font($family);
            }
            if (exists $patterns{$pattern}) {
                $style->set_pattern($patterns{$pattern});
                $mergeStyle->set_pattern($patterns{$pattern});
            }
            $style->set_bold if $bold;
            $style->set_italic if $italic;
            $style->set_size($size) if $size;
            $style->set_underline() if $underline;
            $mergeStyle->set_bold if $bold;
            $mergeStyle->set_italic if $italic;
            $mergeStyle->set_size($size) if $size;
            $mergeStyle->set_underline() if $underline;
            
        }
        elsif ($child->getNodeName eq "Alignment") {
            my $vertical = $child->getAttribute("ss:Vertical");
            my $horizontal = $child->getAttribute("ss:Horizontal");
            my $wrapText = $child->getAttribute("ss:WrapText");
            if ($vertical && exists $verticalAlignments{$vertical}) {
                $style->set_align($verticalAlignments{$vertical});
                $mergeStyle->set_align($verticalAlignments{$vertical});
            }
            if ($horizontal && exists $horizontalAlignments{$horizontal}) {
                $style->set_align($horizontalAlignments{$horizontal});
                $mergeStyle->set_align($horizontalAlignments{$horizontal});
            }
            if ($wrapText) {
                $style->set_text_wrap(1);
                $mergeStyle->set_text_wrap(1);
            }
        }
        elsif ($child->getNodeName eq "Borders") {
            foreach my $border ($child->getChildNodes) {
                if ($border->getNodeName eq "Border") {
                    setBorder($style, $border);
                    setBorder($mergeStyle, $border);
                }
            }
        }
        elsif ($child->getNodeName eq "Interior") {
            my $color = $child->getAttribute("ss:Color");
            my $pattern = $child->getAttribute("ss:Pattern");
            if ($color) {
                my $code = getColor($color);
                if ($code) {
                    $style->set_bg_color($code);
                    $mergeStyle->set_bg_color($code);
                }
            }
            if ($pattern && exists $patterns{$pattern}) {
                $style->set_pattern($patterns{$pattern});
                $mergeStyle->set_pattern($patterns{$pattern});
            }
        }
        elsif ($child->getNodeName eq "NumberFormat") {
            my $format = $child->getAttribute("ss:Format");
            if ($format) {
                $style->set_num_format($format);
                $mergeStyle->set_num_format($format);
            }
        }
    }
    if ($style && $styleId) {
        $styles{$styleId} = $style;
        $mergeStyles{$styleId} = $mergeStyle;
    }
}

sub getTextContent {
    my $node = shift;
    my $text = "";
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeType eq TEXT_NODE) {
            $text .= $child->getData;
        }
    }
    $text;
}

sub addCell {
    my ($sheet, $node, $rowIndex, $colIndex, $inheritedStyleId) = @_;
    my $href = $node->getAttribute("ss:HRef");
    my $styleId = $node->getAttribute("ss:StyleID") || $inheritedStyleId;;
    my $style = $styles{$styleId};
    my $mergeStyle = $mergeStyles{$styleId} || $defaultStyle;
    my $mergeAcross = $node->getAttribute("ss:MergeAcross");
    my $mergeDown = $node->getAttribute("ss:MergeDown");
    my $formula = $node->getAttribute("ss:Formula");
    $rowIndex--;
    $colIndex--;
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeName eq "Data") {
            my $data = getTextContent($child);
            my $type = $child->getAttribute("ss:Type");
            if ($mergeAcross || $mergeDown) {
                my $lastRow = $rowIndex + $mergeDown;
                my $lastCol = $colIndex + $mergeAcross;
                if ($href) {

                    # Handling of merged cells with URLs is not well
                    # documented, but this seems to work.
                    $sheet->write_url($rowIndex, $colIndex, $href, $data,
                                      $style);
                    
                    # Pad out the rest of the area with formatted blank cells.
                    for my $r ($rowIndex .. $lastRow) {
                        for my $c ($colIndex .. $lastCol) {
                            next if $r == $rowIndex and $c == $colIndex;
                            $sheet->write_blank($r, $c, $style);
                        }
                    }

                    $sheet->merge_cells($rowIndex, $colIndex,
                                        $lastRow, $lastCol);
                }
                else {
                    $sheet->merge_range($rowIndex, $colIndex,
                                        $lastRow, $lastCol,
                                        $data, $mergeStyle);
                }
            }
            elsif ($formula) {
                $sheet->write_formula($rowIndex, $colIndex, $formula, $style,
                                      $data);
            }
            elsif ($href) {
                $sheet->write_url($rowIndex, $colIndex, $href, $data, $style);
            }
            elsif ($type eq "DateTime") {
                $sheet->write_date_time($rowIndex, $colIndex, $data, $style);
            }
            elsif ($type eq "Number") {
                $sheet->write_number($rowIndex, $colIndex, $data, $style);
            }
            else {
                $sheet->write_string($rowIndex, $colIndex, $data, $style);
            }
        }
    }
}
            
sub addRow {
    my ($sheet, $node, $rowIndex, $styleId) = @_;
    my $colIndex = 1;
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeName eq "Cell") {
            my $ssIndex = $child->getAttribute("ss:Index");
            $colIndex = $ssIndex if $ssIndex;
            addCell($sheet, $child, $rowIndex, $colIndex, $styleId);
            $colIndex++;
        }
    }
}

sub setSheetOptions {
    my ($sheet, $node) = @_;
    my ($splitHorizontal, $splitVertical) = (0, 0);
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeName eq "SplitHorizontal") {
            $splitHorizontal = getTextContent($child);
        }
        elsif ($child->getNodeName eq "SplitVertical") {
            $splitVertical = getTextContent($child);
        }
    }
    if ($splitHorizontal || $splitVertical) {
        $sheet->freeze_panes($splitHorizontal, $splitVertical);
    }
}
        
sub addWorksheet {
    my ($book, $node) = @_;
    my $name  = $node->getAttribute("ss:Name");
    my $sheet = $book->add_worksheet($name);
    foreach my $child ($node->getChildNodes) {
        if ($child->getNodeName eq "Table") {
            my $defaultHeight = $child->getAttribute("ss:DefaultRowHeight");
            my $tableStyleId = $child->getAttribute("ss:StyleID");
            my $rowIndex = 1;
            my $colIndex = 1;
            foreach my $grandchild ($child->getChildNodes) {
                if ($grandchild->getNodeName eq "Row") {
                    my $height = $grandchild->getAttribute("ss:Height");
                    my $ssIndex = $grandchild->getAttribute("ss:Index");
                    my $styleId = $grandchild->getAttribute("ss:StyleID");
                    if (!$height) {
                        if ($defaultHeight) {
                            $height = $defaultHeight;
                        }
                        else {
                            $height = undef;
                        }
                    }
                    $rowIndex = $ssIndex if $ssIndex;
                    $styleId  = $tableStyleId if !$styleId;
                    my $style = $styles{$styleId};
                    if ($height) { # || $style) { bug in package
                        $sheet->set_row($rowIndex - 1, $height, $style);
                    }
                    addRow($sheet, $grandchild, $rowIndex, $styleId);
                    $rowIndex++;
                }
                elsif ($grandchild->getNodeName eq "Column") {
                    my $width = $grandchild->getAttribute("ss:Width");
                    my $ssIndex = $grandchild->getAttribute("ss:Index");
                    if ($ssIndex) {
                        $colIndex = $ssIndex;
                    }
                    if ($width) {
                        my $c = $colIndex - 1;
                        $sheet->set_column($c, $c, $width * .1829); # / 8.64);
                    }
                    $colIndex++;
                }
            }
        }
        elsif ($child->getNodeName eq "WorksheetOptions") {
            setSheetOptions($sheet, $child);
        }
    }
}

foreach my $node ($doc->getDocumentElement->getChildNodes) {
    if ($node->getNodeName eq "Styles") {
        foreach my $child ($node->getChildNodes) {
            if ($child->getNodeName eq "Style") {
                addStyle($book, $child);
            }
        }
    }
}

foreach my $node ($doc->getDocumentElement->getChildNodes) {
    if ($node->getNodeName eq "Worksheet") {
        addWorksheet($book, $node);
    }
}

$book->close();