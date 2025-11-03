App
· python
#
        ]
        csat_candidates = ["Média CSAT","media csat","avg","media","CSAT","csat","CSAT Médio","csat médio"]


        rec_counts, rec_scores = [], []


        for mkey, payload in sorted(months_dict.items()):
            df = payload.get("by_channel")
            if not isinstance(df, pd.DataFrame) or df.empty:
                for v in payload.values():
                    if isinstance(v, pd.DataFrame) and "Canal" in normalize_canal_column(v).columns:
                        df = normalize_canal_column(v)
                        break
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue


            df = normalize_canal_column(df.copy())
            colmap = {str(c).strip().lower(): c for c in df.columns}


            # contagem de respostas CSAT
            ccol = None
            for c in count_candidates:
                if c.lower() in colmap: ccol = colmap[c.lower()]; break
            if ccol is not None:
                tmp = df[["Canal", ccol]].copy()
                tmp[ccol] = pd.to_numeric(tmp[ccol], errors="coerce")
                tmp = tmp.dropna()
                if not tmp.empty:
                    tmp = tmp.rename(columns={ccol: "Respostas CSAT"})
                    tmp["mes"] = mkey
                    rec_counts.append(tmp)


            # média csat (filtrar <= 3.0)
            scol = None
            for c in csat_candidates:
                if c.lower() in colmap: scol = colmap[c.lower()]; break
            if scol is not None:
                tmp2 = df[["Canal", scol]].copy()
                tmp2[scol] = pd.to_numeric(tmp2[scol], errors="coerce")
                tmp2 = tmp2.dropna()
                if not tmp2.empty:
                    tmp2 = tmp2.rename(columns={scol: "Média CSAT"})
                    tmp2 = tmp2[tmp2["Média CSAT"] <= 3.0]  # <= 3.0 (notas 1,2,3)
                    tmp2["mes"] = mkey
                    rec_scores.append(tmp2)


        colA, colB = st.columns(2)


        with colA:
            st.markdown("**Menor quantidade de respostas do CSAT por mês**")
            n_counts = st.number_input("Quantos canais exibir (menores quantidades)?", 1, 10, 3, 1, key="n_counts_new")
            if not rec_counts:
                st.warning("Não encontrei coluna de contagem de respostas por canal nos dados persistidos.")
            else:
                dd = pd.concat(rec_counts, ignore_index=True)
                tops = [g.sort_values("Respostas CSAT", ascending=True).head(int(n_counts)) for _, g in dd.groupby("mes", as_index=False)]
                dd_top = pd.concat(tops, ignore_index=True)
                st.plotly_chart(px.bar(dd_top, x="mes", y="Respostas CSAT", color="Canal",
                                       barmode="group", title="Menores quantidades de respostas (CSAT) por mês"),
                                use_container_width=True)
                st.dataframe(dd_top.sort_values(["mes","Respostas CSAT","Canal"]), use_container_width=True)


        with colB:
            st.markdown("**Canais com menores notas de CSAT (≤ 3.0) por mês**")
            n_scores = st.number_input("Quantos canais exibir (piores notas)?", 1, 10, 3, 1, key="n_scores_new")
            if not rec_scores:
                st.info("Não encontrei coluna de 'Média CSAT' por canal, ou não há notas ≤ 3.0.")
            else:
                dd2 = pd.concat(rec_scores, ignore_index=True)
                tops2 = [g.sort_values("Média CSAT", ascending=True).head(int(n_scores)) for _, g in dd2.groupby("mes", as_index=False)]
                dd2_top = pd.concat(tops2, ignore_index=True)
                st.plotly_chart(px.bar(dd2_top, x="mes", y="Média CSAT", color="Canal",
                                       barmode="group", title="Menores notas de CSAT (≤ 3.0) por mês"),
                                use_container_width=True)
                st.dataframe(dd2_top.sort_values(["mes","Média CSAT","Canal"]), use_container_width=True)
