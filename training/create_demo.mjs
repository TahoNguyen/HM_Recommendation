// Own vector illustrations and synthetic fixtures; never presented as Kaggle results.
import fs from 'node:fs';
import path from 'node:path';
const root=process.cwd();
const garments={
shirt:`<path d="M114 77 158 58 200 77 242 58 286 77 336 127 297 167 277 147 281 341 119 341 123 147 103 167 64 127Z"/><path d="m158 58 42 19-27 38-30-33m99-24-42 19 27 38 30-33" fill="#ffffff90"/><path d="M200 105v236M138 159h42v40h-42" fill="none" stroke="#ffffff85" stroke-width="3"/><g fill="#fff">${[132,170,208,246,284,321].map(y=>`<circle cx="200" cy="${y}" r="3"/>`).join('')}</g>`,
tee:`<path d="m138 71 35-10q27 27 54 0l35 10 77 64-43 54-39-24v168H143V165l-39 24-43-54Z"/><path d="M173 63q27 35 54 0" fill="none" stroke="#ffffff99" stroke-width="7"/><path d="M148 313h104" stroke="#ffffff44" stroke-width="3"/>`,
pants:`<path d="M128 65h144l12 106-35 184-53-6 6-173-9 173-54 6-22-184Z"/><path d="M128 90h144m-72-24v76m-65-42q0 41-16 42m142-42q0 41 20 42" fill="none" stroke="#ffffff70" stroke-width="3"/>`,
bag:`<path d="M132 159q0-103 68-103t68 103" fill="none" stroke="currentColor" stroke-width="15"/><path d="m113 150-18 183q105 24 210 0l-18-183Z"/><path d="M125 176h150m-60 0v157" fill="none" stroke="#ffffff44" stroke-width="3"/><circle cx="200" cy="204" r="9" fill="#e4c793"/>`,
dress:`<path d="m161 65 12 50h54l12-50 29 16-18 72 56 185q-106 26-212 0l56-185-18-72Z"/><path d="M150 151h100m-80 11-33 160m90-160 33 160" fill="none" stroke="#ffffff70" stroke-width="3"/>`,
coat:`<path d="m157 58 43 23 43-23 47 33 41 224-44 9-24-157 9 179H128l9-179-24 157-44-9 41-224Z"/><path d="m157 58-19 56 62 41-25-58m68-39 19 56-62 41 25-58M200 153v193" fill="none" stroke="#ffffff60" stroke-width="3"/><path d="M143 225h35v29h-35m80-29h35v29h-35" fill="none" stroke="#ffffff70" stroke-width="3"/>`,
shoes:`<path d="m95 206 44-95 49 18 18 100 104 35q24 16 9 37H88q-20-24 7-95Z"/><path d="M89 294h230M152 171l44-4m-49 19 52-2m-58 20 61-2" fill="none" stroke="#ffffffbb" stroke-width="7"/><path d="M211 231q3 36 34 48" fill="none" stroke="#ffffff50" stroke-width="3"/>`,
skirt:`<path d="M144 79h112l48 251q-104 39-208 0Z"/><path d="M143 102h114m-95 11-20 214m55-214v222m40-222 20 214" fill="none" stroke="#ffffff70" stroke-width="3"/>`};
const items=[
['Sơ mi Oxford xanh biển','shirt','#8ab8d6','Garment Upper body','Shirt','Xanh biển',329000],
['Quần chino màu cát','pants','#c5b391','Garment Lower body','Trousers','Be cát',459000],
['Túi everyday màu caramel','bag','#ad7d55','Accessories','Bag','Caramel',289000],
['Áo thun cotton trắng','tee','#dddeda','Garment Upper body','T-shirt','Trắng',189000],
['Đầm midi xanh dịu','dress','#668f9c','Garment Full body','Dress','Xanh dịu',549000],
['Áo khoác linen tự nhiên','coat','#c8bdab','Garment Upper body','Jacket','Be',689000],
['Sneaker xanh navy','shoes','#345570','Shoes','Sneakers','Navy',599000],
['Chân váy xếp ly','skirt','#a4adc5','Garment Lower body','Skirt','Xanh khói',359000],
['Sơ mi relaxed trắng kem','shirt','#d9d7c7','Garment Upper body','Shirt','Trắng kem',349000],
['Quần straight xanh navy','pants','#46657d','Garment Lower body','Trousers','Navy',499000],
['Túi tote xanh biển','bag','#739bb8','Accessories','Bag','Xanh biển',249000],
['Áo thun basic xanh','tee','#829fb0','Garment Upper body','T-shirt','Xanh',199000],
['Đầm everyday màu đất','dress','#b39281','Garment Full body','Dress','Nâu đất',499000],
['Áo khoác xanh navy','coat','#48647d','Garment Upper body','Jacket','Navy',729000],
['Sneaker trắng kem','shoes','#c9c6ba','Shoes','Sneakers','Trắng kem',549000],
['Chân váy linen be','skirt','#c6baa2','Garment Lower body','Skirt','Be',389000]
];
const products=items.map(([name,kind,color,category,type,colour,amount],i)=>{
 const filename=`product-${i+1}.svg`;
 const svg=`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 400 430"><defs><filter id="shadow"><feDropShadow dx="0" dy="9" stdDeviation="8" flood-opacity=".12"/></filter><linearGradient id="fabric" x2="1" y2="1"><stop stop-color="${color}"/><stop offset="1" stop-color="${color}" stop-opacity=".8"/></linearGradient></defs><rect width="400" height="430" fill="${i%2?'#eeeae4':'#edf1f3'}"/><ellipse cx="200" cy="373" rx="101" ry="12" fill="#22394a" opacity=".05"/><g fill="url(#fabric)" color="${color}" filter="url(#shadow)">${garments[kind]}</g></svg>`;
 fs.writeFileSync(path.join(root,'frontend/assets',filename),svg);
 return {article_id:String(1000000000+i),name,category,product_type:type,colour,demo_price_vnd:amount,purchase_count:420-i*19,image_url:'/assets/'+filename,description:'Sản phẩm minh họa cho OCEAN wardrobe. Ảnh là hình vẽ thiết kế riêng; thông tin và giá không phải dữ liệu H&M thật.'};
});
fs.copyFileSync(path.join(root,'frontend/assets/product-1.svg'),path.join(root,'frontend/assets/demo-shirt.svg'));
fs.copyFileSync(path.join(root,'frontend/assets/product-3.svg'),path.join(root,'frontend/assets/demo-bag.svg'));
const rules=[];
for(let i=0;i<products.length;i++)for(const offset of [1,2,6]){rules.push({antecedent:[products[i].article_id],consequent:[products[(i+offset)%products.length].article_id],confidence:0.62-offset*.04,lift:2.8-offset*.15,support:0.022-offset*.002});}
rules.push({antecedent:[products[0].article_id,products[1].article_id],consequent:[products[14].article_id],confidence:.78,lift:3.6,support:.013});
const fixture={products,rules,metrics:{run_id:'demo-synthetic',trained_at:null,transactions:5200,baskets:1800,products:products.length,rules:rules.length,itemsets:82,date_range:null,parameters:{min_support:.0001,min_confidence:.3,min_lift:1},evaluation:null,top_categories:[...new Set(products.map(p=>p.category))].map(category=>({category,count:products.filter(p=>p.category===category).reduce((s,p)=>s+p.purchase_count,0)}))}};
fs.writeFileSync(path.join(root,'data/demo.json'),JSON.stringify(fixture,null,2));
console.log('Created 16 synthetic products, own SVG illustrations and 49 demo rules.');
