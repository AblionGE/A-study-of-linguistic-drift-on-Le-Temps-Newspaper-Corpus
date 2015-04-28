clear all;

A = csvread('distance1/d1.csv');

Im = zeros(159, 159);

for i = 1:size(A,1) 
    row = A(i,1) - 1839;
    col = A(i,2) - 1839;
    
    Im(row, col) = A(i,3);
    
%     if(row ~= col)
%         Im(col, row) = A(i,3);
%     end
end


imagesc(Im); colorbar;
set(gca,'XTick',[1 20 40 60 80 100 120 140] );
set(gca,'YTick',[1 20 40 60 80 100 120 140] );
set(gca,'XTickLabel',[1840 1860 1880 1900 1920 1940 1960 1980] );
set(gca,'YTickLabel',[1840 1860 1880 1900 1920 1940 1960 1980] );